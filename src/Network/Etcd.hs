{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-|

This module contains an implementation of the etcd client.

-}

module Network.Etcd
    ( Client(..)
    , Credentials
    , createClient

      -- * Types
    , Node(..)
    , Index
    , Key
    , Value
    , TTL
    , EtcdException(..)

      -- * Low-level key operations
    , get
    , set
    , getEither
    , create
    , delete
    , compareAndSwap
--  , wait
--  , waitIndex
--  , waitRecursive
--  , waitIndexRecursive

      -- * Directory operations
    , createDirectory
    , listDirectoryContents
    , listDirectoryContentsRecursive
    , removeDirectory
    , removeDirectoryRecursive
    ) where


import           Data.Aeson hiding (Value, Error)
import           Data.Time.Clock
import           Data.Time.LocalTime
import           Data.List hiding (delete)
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Text.Encoding
import           Data.Monoid
import           Data.Maybe
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString as BL

import           Control.Applicative
import           Control.Exception
import           Control.Monad

import qualified Network.HTTP.Types.Status as Http
import           Network.HTTP.Conduit hiding (Response, path)

import           Prelude


-- | The 'Client' holds all data required to make requests to the etcd
-- cluster. You should use 'createClient' to initialize a new client.
data Client = Client
    { leaderUrl :: !Text
      -- ^ The URL to the leader. HTTP requests are sent to this server.
    , credentials :: Maybe Credentials
    }

-- | Exception within the Etcd library.
data EtcdException = InvalidResponse Text
                   | KeyNotFound
                   | PreconditionFailed
                   | ConnectionError HttpException
                   deriving Show
instance Exception EtcdException

convertToEtcdException :: HttpException -> EtcdException
convertToEtcdException ex@(StatusCodeException status _ _) = 
   if status == Http.status404 
      then KeyNotFound 
      else if status == Http.status412
         then PreconditionFailed
         else ConnectionError ex
convertToEtcdException ex = ConnectionError ex

handleHttpException :: HttpException -> IO Response
handleHttpException = throwIO . convertToEtcdException

-- | The version prefix used in URLs. The current client supports v2.
versionPrefix :: Text
versionPrefix = "v2"


-- | The URL to the given key.
keyUrl :: Client -> Key -> Text
keyUrl client key = leaderUrl client <> "/" <> versionPrefix <> "/keys/" <> key


------------------------------------------------------------------------------
-- | Each response comes with an "action" field, which describes what kind of
-- action was performed.
data Action = GET | SET | DELETE | CREATE | EXPIRE | CAS | CAD
    deriving (Show, Eq, Ord)


instance FromJSON Action where
    parseJSON (String "get")              = return GET
    parseJSON (String "set")              = return SET
    parseJSON (String "delete")           = return DELETE
    parseJSON (String "create")           = return CREATE
    parseJSON (String "expire")           = return EXPIRE
    parseJSON (String "compareAndSwap")   = return CAS
    parseJSON (String "compareAndDelete") = return CAD
    parseJSON _                           = fail "Action"



------------------------------------------------------------------------------
-- | The server responds with this object to all successful requests.
data Response = Response
    { _resAction   :: Action
    , _resNode     :: Node
    , _resPrevNode :: Maybe Node
    } deriving (Show, Eq, Ord)


instance FromJSON Response where
    parseJSON (Object o) = Response
        <$> o .:  "action"
        <*> o .:  "node"
        <*> o .:? "prevNode"

    parseJSON _ = fail "Response"



------------------------------------------------------------------------------
-- | The server sometimes responds to errors with this error object.
data Error = Error !Text
    deriving (Show, Eq, Ord)


-- | The etcd index is a unique, monotonically-incrementing integer created for
-- each change to etcd. See etcd documentation for more details.
type Index = Int


-- | Keys are strings, formatted like filesystem paths (ie. slash-delimited
-- list of path components).
type Key = Text


-- | Values attached to leaf nodes are strings. If you want to store
-- structured data in the values, you'll need to encode it into a string.
type Value = Text


-- | TTL is specified in seconds. The server accepts negative values, but they
-- don't make much sense.
type TTL = Int

-- | The 'Node' corresponds to the node object as returned by the etcd API.
--
-- There are two types of nodes in etcd. One is a leaf node which holds
-- a value, the other is a directory node which holds zero or more child nodes.
-- A directory node can not hold a value, the two types are exclusive.
--
-- On the wire, the two are not really distinguished, except that the JSON
-- objects have different fields.
--
-- A node may be set to expire after a number of seconds. This is indicated by
-- the two fields 'ttl' and 'expiration'.

data Node = Node
    { _nodeKey           :: !Key
      -- ^ The key of the node. It always starts with a slash character (0x47).

    , _nodeCreatedIndex  :: !Index
      -- ^ A unique index, reflects the point in the etcd state machine at
      -- which the given key was created.

    , _nodeModifiedIndex :: !Index
      -- ^ Like '_nodeCreatedIndex', but reflects when the node was last
      -- changed.

    , _nodeDir           :: !Bool
      -- ^ 'True' if this node is a directory.

    , _nodeValue         :: !(Maybe Value)
      -- ^ The value is only present on leaf nodes. If the node is
      -- a directory, then this field is 'Nothing'.

    , _nodeNodes         :: !(Maybe [Node])
      -- ^ If this node is a directory, then these are its children. The list
      -- may be empty.

    , _nodeTTL           :: !(Maybe TTL)
      -- ^ If the node has TTL set, this is the number of seconds how long the
      -- node will exist.

    , _nodeExpiration    :: !(Maybe UTCTime)
      -- ^ If TTL is set, then this is the time when it expires.

    } deriving (Show, Eq, Ord)


instance FromJSON Node where
    parseJSON (Object o) = Node
        <$> o .:? "key" .!= "/"
        <*> o .:? "createdIndex" .!= 0
        <*> o .:? "modifiedIndex" .!= 0
        <*> o .:? "dir" .!= False
        <*> o .:? "value"
        <*> o .:? "nodes"
        <*> o .:? "ttl"
        <*> (fmap zonedTimeToUTC <$> (o .:? "expiration"))

    parseJSON _ = fail "Response"

-- -- | The 'HNode' is a high level version of node
--
-- data HNode
--    = Leaf { _leafKey :: !Key, _leafValue :: !Value }
--    | HNode { _hNodeKey :: !Key, _hNodeChildren :: [HNode] }
--
-- toHNode :: Node -> HNode
-- toHNode (Node{..}) =
--    case _nodeNodes of
--       Just nodes -> HNode { _hNodeKey = _nodeKey, _hNodeChildren = map toHNode nodes }
--       Nothing    -> Leaf { _leafKey = _nodeKey, _leafValue = fromMaybe "" _nodeValue }

{-|---------------------------------------------------------------------------

Low-level HTTP interface

The functions here are used internally when sending requests to etcd. If the
server is running, the result is 'Either Error Response'. These functions may
throw an exception if the server is unreachable or not responding.

-}


-- A type synonym for a http response.
type HR = Either Error Response

type Credentials = (BL.ByteString, BL.ByteString)

decodeResponseBody :: ByteString -> IO Response
decodeResponseBody body =
   either (throwIO . InvalidResponse . T.pack) return $ eitherDecode body

maybeAuthenticate :: Maybe Credentials -> Request -> Request
maybeAuthenticate mcred req =
   maybe req (\(u, p) -> applyBasicAuth u p req) mcred

httpGET :: Text -> [(Text, Text)] -> Maybe Credentials -> IO Response
httpGET url params mcred = handle handleHttpException $ do
    req'  <- acceptJSON <$> parseUrl (T.unpack url)
    let req = setQueryString (map (\(k,v) -> (encodeUtf8 k, Just $ encodeUtf8 v)) params) $ req'
        authReq = maybeAuthenticate mcred req
    res <- withManager $ httpLbs authReq
    decodeResponseBody $ responseBody res
  where
    acceptHeader   = ("Accept","application/json")
    acceptJSON req = req { requestHeaders = acceptHeader : requestHeaders req }

httpPUT :: Text -> [(Text, Text)] -> Maybe Credentials -> IO Response
httpPUT url params mcred = handle handleHttpException $ do
    req' <- parseUrl (T.unpack url)
    let req = urlEncodedBody (map (\(k,v) -> (encodeUtf8 k, encodeUtf8 v)) params) $ req'
        authReq = maybeAuthenticate mcred req
    res <- withManager $ httpLbs authReq { method = "PUT" }
    decodeResponseBody $ responseBody res

httpPOST :: Text -> [(Text, Text)] -> Maybe Credentials -> IO Response
httpPOST url params mcred = handle handleHttpException $ do
    req' <- parseUrl (T.unpack url)
    let req = urlEncodedBody (map (\(k,v) -> (encodeUtf8 k, encodeUtf8 v)) params) $ req'
        authReq = maybeAuthenticate mcred req
    res <- withManager $ httpLbs authReq { method = "POST" }
    decodeResponseBody $ responseBody res

-- | Issue a DELETE request to the given url. Since DELETE requests don't have
-- a body, the params are appended to the URL as a query string.
httpDELETE :: Text -> [(Text, Text)] -> Maybe Credentials -> IO Response
httpDELETE url params mcred = handle handleHttpException $ do
    req  <- parseUrl $ T.unpack $ url <> (asQueryParams params)
    let authReq = maybeAuthenticate mcred req
    res <- withManager $ httpLbs authReq { method = "DELETE" }
    decodeResponseBody $ responseBody res
  where
    asQueryParams [] = ""
    asQueryParams xs = "?" <> mconcat (intersperse "&" (map (\(k,v) -> k <> "=" <> v) xs))


------------------------------------------------------------------------------
-- | Run a low-level HTTP request. Catch any exceptions and convert them into
-- a 'Left Error'.
runRequest :: IO HR -> IO HR
runRequest a = catch a (\(e :: SomeException) -> return $ Left $ Error $ T.pack $ show e)

-- | Encode an optional TTL into a param pair.
ttlParam :: Maybe TTL -> [(Text, Text)]
ttlParam Nothing    = []
ttlParam (Just ttl) = [("ttl", T.pack $ show ttl)]



{-----------------------------------------------------------------------------

Public API

-}


-- | Create a new client and initialize it with a list of seed machines. If
-- the list is empty Nothing is returned
createClient :: [ Text ] -> Maybe Credentials -> IO (Maybe Client)
createClient seed mcred = 
   return $ fmap (\f -> f mcred) (Client <$> (listToMaybe seed))


{-----------------------------------------------------------------------------

Low-level key operations

-}

waitParam :: (Text, Text)
waitParam = ("wait","true")

waitRecursiveParam :: (Text, Text)
waitRecursiveParam = ("recursive","true")

waitIndexParam :: Index -> (Text, Text)
waitIndexParam i = ("waitIndex", (T.pack $ show i))


-- | Get the node at the given key.
get :: Client -> Key -> IO Node
get client key = do
    res <- httpGET (keyUrl client key) [] $ credentials client
    return $ _resNode res
        
-- | Get the node at the given key.
getEither :: Client -> Key -> IO (Either Text Node)
getEither client key = handle handleExceptions $ do
    res <- httpGET (keyUrl client key) [] $ credentials client
    return $ Right $ _resNode res
    where 
      handleExceptions :: EtcdException -> IO (Either Text Node)
      handleExceptions = return . Left . T.pack . show

-- | Set the value at the given key.
set :: Client -> Key -> Value -> Maybe TTL -> IO Node
set client key value mbTTL = do
    res <- httpPUT (keyUrl client key) ([("value",value)] ++ ttlParam mbTTL) $ credentials client
    return $ _resNode res

-- | Create a value in the given key. The key must be a directory.
create :: Client -> Key -> Value -> Maybe TTL -> IO Node
create client key value mbTTL = do
    res <- httpPOST (keyUrl client key) ([("value",value)] ++ ttlParam mbTTL) $ credentials client
    return $ _resNode res

-- | Detele the given key.
delete :: Client -> Key -> IO ()
delete client key = 
   void $ httpDELETE (keyUrl client key) [] (credentials client)

-- | Atomic compare-and-swap with 'prevValue' compare only.
-- The key must not be a directory.
compareAndSwap :: Client   -- ^ Etcd client
               -> Key      -- ^ Key that isn't a directory
               -> Value    -- ^ Previous value
               -> Value    -- ^ New value
               -> IO Node
compareAndSwap client key prevValue value = do
   res <- httpPUT (keyUrl client key) [("prevValue", prevValue), ("value", value)] (credentials client)
   return $ _resNode res

{-
-- | Wait for changes on the node at the given key.
wait :: Client -> Key -> IO (Maybe Node)
wait client key =
    runRequest' $ httpGET (keyUrl client key) [waitParam]


-- | Same as 'wait' but at a given index.
waitIndex :: Client -> Key -> Index -> IO (Maybe Node)
waitIndex client key index =
    runRequest' $ httpGET (keyUrl client key) $
        [waitParam, waitIndexParam index]


-- | Same as 'wait' but includes changes on children.
waitRecursive :: Client -> Key -> IO (Maybe Node)
waitRecursive client key =
    runRequest' $ httpGET (keyUrl client key) $
        [waitParam, waitRecursiveParam]


-- | Same as 'waitIndex' but includes changes on children.
waitIndexRecursive :: Client -> Key -> Index -> IO (Maybe Node)
waitIndexRecursive client key index =
    runRequest' $ httpGET (keyUrl client key) $
        [waitParam, waitIndexParam index, waitRecursiveParam]


-}
{-----------------------------------------------------------------------------

Directories are non-leaf nodes which contain zero or more child nodes. When
manipulating directories one must include dir=true in the request params.

-}

dirParam :: [(Text, Text)]
dirParam = [("dir","true")]

recursiveParam :: [(Text, Text)]
recursiveParam = [("recursive","true")]


-- | Create a directory at the given key.
createDirectory :: Client -> Key -> Maybe TTL -> IO ()
createDirectory client key mbTTL =
    void $ httpPUT (keyUrl client key) (dirParam ++ ttlParam mbTTL) (credentials client)


-- | List all nodes within the given directory.
listDirectoryContents :: Client -> Key -> IO [Node]
listDirectoryContents client key = handle handleException $ do
    res <- httpGET (keyUrl client key) [] $ credentials client
    let node = _resNode res
    case _nodeNodes node of
       Nothing -> return []
       Just children -> return children
   where
      handleException :: EtcdException -> IO [Node]
      handleException KeyNotFound = return []
      handleException ex = throwIO ex


-- | Same as 'listDirectoryContents' but includes all descendant nodes. Note
-- that directory 'Node's will not contain their children.
listDirectoryContentsRecursive :: Client -> Key -> IO [Node]
listDirectoryContentsRecursive client key = handle handleExceptions $ do
    res <- httpGET (keyUrl client key) recursiveParam $ credentials client
    let node = _resNode res
        flatten n = n { _nodeNodes = Nothing }
                 : maybe [] (concatMap flatten) (_nodeNodes n)
    case _nodeNodes node of
       Nothing -> return []
       Just children -> return $ concatMap flatten children
   where
      handleExceptions :: EtcdException -> IO [Node]
      handleExceptions KeyNotFound = return []
      handleExceptions ex = throwIO ex


-- | Remove the directory at the given key. The directory MUST be empty,
-- otherwise the removal fails. If you don't care about the keys within, you
-- can use 'removeDirectoryRecursive'.
removeDirectory :: Client -> Key -> IO ()
removeDirectory client key =
    void $ httpDELETE (keyUrl client key) dirParam $ credentials client


-- | Remove the directory at the given key, including all its children.
removeDirectoryRecursive :: Client -> Key -> IO ()
removeDirectoryRecursive client key =
    void $ httpDELETE (keyUrl client key) (dirParam ++ recursiveParam) $ credentials client
