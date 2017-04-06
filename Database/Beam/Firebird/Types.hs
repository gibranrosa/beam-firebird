{-# LANGUAGE TypeOperators, ConstraintKinds #-}

module Database.Beam.Firebird.Types where

import           Database.Beam.Backend.Types
import           Database.Beam.Backend.SQL

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import           Data.Int (Int32, Int64)
import           Data.Text (Text)
import qualified Data.Text.Lazy as TL
import           Data.Time (UTCTime, LocalTime, Day, utcToLocalTime, utc)
import           Data.Word (Word32, Word64)

import           Database.Beam.Firebird.FromRow
--import qualified Database.SQLite.Simple.FromField as Sql
--import qualified Database.SQLite.Simple.Types as Sql

data Firebird = Firebird


instance BeamBackend Firebird where
  type BackendFromField Firebird = FromField

instance FromBackendRow Firebird Bool
instance FromBackendRow Firebird Double
--instance FromBackendRow Firebird Float
instance FromBackendRow Firebird Int
--instance FromBackendRow Firebird Int8
--instance FromBackendRow Firebird Int16
instance FromBackendRow Firebird Int32
instance FromBackendRow Firebird Int64
instance FromBackendRow Firebird Integer
instance FromBackendRow Firebird Rational
--instance FromBackendRow Firebird Word
--instance FromBackendRow Firebird Word8
--instance FromBackendRow Firebird Word16
instance FromBackendRow Firebird Word32
instance FromBackendRow Firebird Word64
instance FromBackendRow Firebird ByteString
instance FromBackendRow Firebird BL.ByteString
instance FromBackendRow Firebird Text
instance FromBackendRow Firebird TL.Text
instance FromBackendRow Firebird UTCTime
instance FromBackendRow Firebird Day
--instance FromBackendRow Firebird O.SqlNull
instance FromBackendRow Firebird LocalTime where
  fromBackendRow = utcToLocalTime utc <$> fromBackendRow

--instance Sql.FromField x => Sql.FromField (Auto x) where
  --fromField field = fmap (Auto . Just) (Sql.fromField field)

instance BeamSqlBackend Firebird
instance BeamSql92Backend Firebird

