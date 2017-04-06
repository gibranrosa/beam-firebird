{-# LANGUAGE TypeOperators, ConstraintKinds, DeriveDataTypeable, GeneralizedNewtypeDeriving,
             DeriveFunctor #-}


module Database.Beam.Firebird.Internal where

import           Database.Beam.Firebird.Ok
import           Control.Exception (Exception)
import qualified Database.HDBC as O

import           Data.ByteString (ByteString)
import           Data.Typeable (Typeable)
import           Control.Monad.Trans.State.Strict
import           Control.Monad.Trans.Reader

import           Control.Monad
import           Control.Applicative


-- | A placeholder for the SQL @NULL@ value.
data Null = Null
          deriving (Read, Show, Typeable)

instance Eq Null where
    _ == _ = False
    _ /= _ = False

data ColumnOutOfBounds = ColumnOutOfBounds { errorColumnIndex :: !Int }
                      deriving (Eq, Show, Typeable)

instance Exception ColumnOutOfBounds

data Field = Field {
     result   :: O.SqlValue
   , column   :: {-# UNPACK #-} !Int
   }

-- Named type for holding RowParser read-only state.  Just for making
-- it easier to make sense out of types in FromRow.
newtype RowParseRO = RowParseRO { nColumns :: Int }

newtype RowParser a = RP { unRP :: ReaderT RowParseRO (StateT (Int, [O.SqlValue]) Ok) a }
   deriving ( Functor, Applicative, Alternative, Monad, MonadPlus )

gettypename :: O.SqlValue -> ByteString
gettypename (O.SqlInteger _) = "INTEGER"
gettypename (O.SqlDouble _) = "FLOAT"
gettypename (O.SqlString _) = "TEXT"
gettypename (O.SqlByteString _) = "BLOB"
gettypename O.SqlNull = "NULL"


-- | A single-value \"collection\".
--
-- This is useful if you need to supply a single parameter to a SQL
-- query, or extract a single column from a SQL result.
--
-- Parameter example:
--
-- @query c \"select x from scores where x > ?\" ('Only' (42::Int))@
--
-- Result example:
--
-- @xs <- query_ c \"select id from users\"
--forM_ xs $ \\('Only' id) -> {- ... -}@
newtype Only a = Only {
      fromOnly :: a
    } deriving (Eq, Ord, Read, Show, Typeable, Functor)

-- | A composite type to parse your custom data structures without
-- having to define dummy newtype wrappers every time.
--
--
-- > instance FromRow MyData where ...
--
-- > instance FromRow MyData2 where ...
--
--
-- then I can do the following for free:
--
-- @
-- res <- query' c "..."
-- forM res $ \\(MyData{..} :. MyData2{..}) -> do
--   ....
-- @
data h :. t = h :. t deriving (Eq,Ord,Show,Read,Typeable)

infixr 3 :.

