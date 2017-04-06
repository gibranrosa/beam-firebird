{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Beam.Firebird.Connection where

import           Database.Beam.Backend
import           Database.Beam.Firebird.Syntax
import           Database.Beam.Firebird.Types
import           Database.Beam.Firebird.FromRow
import           Database.Beam.Firebird.ToRow
import           Database.Beam.Firebird.Ok
import           Database.Beam.Firebird.Internal

--import           Database.SQLite.Simple ( Connection, ToRow(..), FromRow(..)
  --                                      , SQLData, field
    --                                    , withStatement, bind, nextRow)
import           Database.HDBC as O
import           Database.HDBC.ODBC as O
--import           Database.SQLite.Simple.Internal (RowParser(RP), unRP)
--import           Database.SQLite.Simple.Ok (Ok(..))
--import           Database.SQLite.Simple.Types (Null)

import           Control.Monad.Free.Church
import           Control.Monad.Reader
import           Control.Monad.State.Strict
import           Control.Monad.Trans
import           Control.Exception
import qualified Data.Text as T

import           Data.ByteString.Builder (toLazyByteString)
import qualified Data.ByteString.Lazy.Char8 as BL
import qualified Data.DList as D
import           Data.String

newtype FirebirdM a = FirebirdM { runFirebirdM :: ReaderT O.Connection IO a }
  deriving (Monad, Functor, Applicative, MonadIO)

newtype BeamFirebirdParams = BeamFirebirdParams [O.SqlValue]
instance ToRow BeamFirebirdParams where
  toRow (BeamFirebirdParams x) = x

newtype BeamFirebirdRow a = BeamFirebirdRow a
instance FromBackendRow Firebird a => FromRow (BeamFirebirdRow a) where
  fromRow = BeamFirebirdRow <$> runF (fromBackendRow :: FromBackendRowM Firebird a) finish step
      where
        finish = pure

        step :: FromBackendRowF Firebird (RowParser a) -> RowParser a
        step (ParseOneField next) =
            field >>= next
        step (PeekField next) =
            RP $ do
              ro <- ask
              st <- get
              case runStateT (runReaderT (unRP field) ro) st of
                Ok (a, _) -> unRP (next (Just a))
                _ -> unRP (next Nothing)
        step (CheckNextNNull n next) =
            RP $ do
              ro <- ask
              st <- get
              case runStateT (runReaderT (unRP (replicateM_ n (field :: RowParser Null))) ro) st of
                Ok ((), _) -> unRP (next True)
                _ -> unRP (next False)

runFirebird :: O.Connection -> FirebirdM a -> IO a
runFirebird conn x = runReaderT (runFirebirdM x) conn

instance MonadBeam FirebirdCommandSyntax Firebird FirebirdM where
  runReturningMany (FirebirdCommandSyntax (FirebirdSyntax cmd vals)) action =
      FirebirdM $ do
        conn <- ask
        liftIO . withStatement conn (fromString (BL.unpack (toLazyByteString cmd))) $ \stmt ->
            do --bind stmt (BeamFirebirdParams (D.toList vals))
               _ <- O.execute stmt $ D.toList vals
               let nextRow' = liftIO (nextRow stmt) >>= \x ->
                              case x of
                                Nothing -> pure Nothing
                                Just (BeamFirebirdRow row) -> pure row
               runReaderT (runFirebirdM (action nextRow')) conn


withStatement :: O.Connection -> String -> (Statement -> IO a) -> IO a 
withStatement conn qry f =
  O.prepare conn qry >>= f 


-- | Extracts the next row from the prepared statement.
nextRow :: (FromRow r) => Statement -> IO (Maybe r)
nextRow = nextRowWith fromRow

nextRowWith :: RowParser r -> Statement -> IO (Maybe r)
nextRowWith fromRow_ stmt = do
  statRes <- O.fetchRow stmt
  case statRes of
    Just rowRes -> do     
      let nCols = length rowRes
      row <- convertRow fromRow_ rowRes nCols
      return $ Just row
    Nothing -> return Nothing

convertRow :: RowParser r -> [O.SqlValue] -> Int -> IO r
convertRow fromRow_ rowRes ncols = do
  let rw = RowParseRO ncols
  case runStateT (runReaderT (unRP fromRow_) rw) (0, rowRes) of
    Ok (val,(col,_))
       | col == ncols -> return val
       | otherwise -> errorColumnMismatch (ColumnOutOfBounds col)
    Errors []  -> throwIO $ ConversionFailed "" "" "unknown error"
    Errors [x] ->
      throw x `Control.Exception.catch` (\e -> errorColumnMismatch (e :: ColumnOutOfBounds))
    Errors xs  -> throwIO $ ManyErrors xs
  where
    errorColumnMismatch :: ColumnOutOfBounds -> IO r
    errorColumnMismatch (ColumnOutOfBounds c) = do
      let vals = map (\f -> (gettypename f, ellipsis f)) rowRes
      throwIO (ConversionFailed
               (show ncols ++ " values: " ++ show vals)
               ("at least " ++ show c ++ " slots in target type")
               "mismatch between number of columns to convert and number in target type")

    ellipsis :: O.SqlValue -> T.Text
    ellipsis sql
      | T.length bs > 20 = T.take 15 bs `T.append` "[...]"
      | otherwise        = bs
      where
        bs = T.pack $ show sql
