{-# LANGUAGE TypeOperators #-}

module Database.Beam.Firebird.FromRow
     ( FromRow(..)
     , RowParser
     , field
     , fieldWith
     , numFieldsRemaining

     , FromField(..)
     , ResultError(..)
     ) where

import           Control.Applicative (Applicative(..), (<$>))
import           Control.Exception (SomeException(..), Exception)
import           Control.Monad (replicateM)
import           Control.Monad.Trans.State.Strict
import           Control.Monad.Trans.Reader
import           Control.Monad.Trans.Class
import           Data.Typeable (Typeable, typeOf)

import           Data.Convertible

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import           Data.Int (Int32, Int64)
import           Data.Text (Text)
import qualified Data.Text.Lazy as TL
import           Data.Time (UTCTime, LocalTime, Day)
import           Data.Word (Word32, Word64)

import           Database.Beam.Firebird.Ok
import           Database.Beam.Firebird.Internal
import qualified Database.HDBC as O

-- | Exception thrown if conversion from a SQL value to a Haskell
-- value fails.
data ResultError = Incompatible { errSQLType :: String
                                , errHaskellType :: String
                                , errMessage :: String }
                 -- ^ The SQL and Haskell types are not compatible.
                 | UnexpectedNull { errSQLType :: String
                                  , errHaskellType :: String
                                  , errMessage :: String }
                 -- ^ A SQL @NULL@ was encountered when the Haskell
                 -- type did not permit it.
                 | ConversionFailed { errSQLType :: String
                                    , errHaskellType :: String
                                    , errMessage :: String }
                 -- ^ The SQL value could not be parsed, or could not
                 -- be represented as a valid Haskell value, or an
                 -- unexpected low-level error occurred (e.g. mismatch
                 -- between metadata and actual data in a row).
                   deriving (Eq, Show, Typeable)

instance Exception ResultError

left :: Exception a => a -> Ok b
left = Errors . (:[]) . SomeException

type FieldParser a = Field -> Ok a

--type FromField = Convertible O.SqlValue
-- | A type that may be converted from a SQL type.
class (Typeable a, Convertible O.SqlValue a) => FromField a where
  fromField :: FieldParser a
  fromField f@(Field sqlVal _) = 
    case safeConvert sqlVal of 
      Left (ConvertError _ _ _ msg) -> returnError ConversionFailed f msg
      Right v -> Ok v

instance Convertible O.SqlValue Null where 
  safeConvert O.SqlNull = return Null

instance FromField Null where
  fromField (Field O.SqlNull _) = pure Null
  fromField f                   = returnError ConversionFailed f "data is not null"
instance FromField Bool 
instance FromField Double
instance FromField Int 
instance FromField Int32
instance FromField Int64
instance FromField Integer
instance FromField Rational
instance FromField Word32
instance FromField Word64
instance FromField ByteString
instance FromField BL.ByteString
instance FromField Text
instance FromField TL.Text
instance FromField UTCTime
instance FromField Day
--instance FromBackendRow Firebird O.SqlNull
instance FromField LocalTime
-- where
  --fromBackendRow = utcToLocalTime utc <$> fromBackendRow

fieldTypename :: Field -> String
fieldTypename = B.unpack . gettypename . result

returnError :: forall a err . (Typeable a, Exception err)
            => (String -> String -> String -> err)
            -> Field -> String -> Ok a
returnError mkErr f = left . mkErr (fieldTypename f)
                                   (show (typeOf (undefined :: a)))


-- | A collection type that can be converted from a sequence of fields.
-- Instances are provided for tuples up to 10 elements and lists of any length.
--
-- Note that instances can defined outside of sqlite-simple,  which is
-- often useful.   For example, here's an instance for a user-defined pair:
--
-- @data User = User { name :: String, fileQuota :: Int }
--
-- instance 'FromRow' User where
--     fromRow = User \<$\> 'field' \<*\> 'field'
-- @
--
-- The number of calls to 'field' must match the number of fields returned
-- in a single row of the query result.  Otherwise,  a 'ConversionFailed'
-- exception will be thrown.
--
-- Note the caveats associated with user-defined implementations of
-- 'fromRow'.

class FromRow a where
    fromRow :: RowParser a

fieldWith :: FieldParser a -> RowParser a
fieldWith fieldP = RP $ do
    ncols <- asks nColumns
    (column, remaining) <- lift get
    lift (put (column + 1, tail remaining))
    if column >= ncols
    then
      lift (lift (Errors [SomeException (ColumnOutOfBounds (column+1))]))
    else do
      let r = head remaining
          field = Field r column
      lift (lift (fieldP field))

field :: FromField a => RowParser a
field = fieldWith fromField

numFieldsRemaining :: RowParser Int
numFieldsRemaining = RP $ do
  ncols <- asks nColumns
  (columnIdx,_) <- lift get
  return $! ncols - columnIdx

instance (FromField a) => FromRow (Only a) where
    fromRow = Only <$> field

instance (FromField a, FromField b) => FromRow (a,b) where
    fromRow = (,) <$> field <*> field

instance (FromField a, FromField b, FromField c) => FromRow (a,b,c) where
    fromRow = (,,) <$> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d) =>
    FromRow (a,b,c,d) where
    fromRow = (,,,) <$> field <*> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e) =>
    FromRow (a,b,c,d,e) where
    fromRow = (,,,,) <$> field <*> field <*> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f) =>
    FromRow (a,b,c,d,e,f) where
    fromRow = (,,,,,) <$> field <*> field <*> field <*> field <*> field
                      <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f, FromField g) =>
    FromRow (a,b,c,d,e,f,g) where
    fromRow = (,,,,,,) <$> field <*> field <*> field <*> field <*> field
                       <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f, FromField g, FromField h) =>
    FromRow (a,b,c,d,e,f,g,h) where
    fromRow = (,,,,,,,) <$> field <*> field <*> field <*> field <*> field
                        <*> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f, FromField g, FromField h, FromField i) =>
    FromRow (a,b,c,d,e,f,g,h,i) where
    fromRow = (,,,,,,,,) <$> field <*> field <*> field <*> field <*> field
                         <*> field <*> field <*> field <*> field

instance (FromField a, FromField b, FromField c, FromField d, FromField e,
          FromField f, FromField g, FromField h, FromField i, FromField j) =>
    FromRow (a,b,c,d,e,f,g,h,i,j) where
    fromRow = (,,,,,,,,,) <$> field <*> field <*> field <*> field <*> field
                          <*> field <*> field <*> field <*> field <*> field

instance FromField a => FromRow [a] where
    fromRow = do
      n <- numFieldsRemaining
      replicateM n field

instance (FromRow a, FromRow b) => FromRow (a :. b) where
    fromRow = (:.) <$> fromRow <*> fromRow
