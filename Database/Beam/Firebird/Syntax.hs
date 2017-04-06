module Database.Beam.Firebird.Syntax
  ( FirebirdSyntax(..)

  , FirebirdCommandSyntax(..) ) where

import           Database.Beam.Backend.SQL

import           Data.ByteString (ByteString)
import           Data.ByteString.Builder
import           Data.Coerce
import qualified Data.DList as DL
import           Data.Monoid
import           Data.String
import qualified Data.Text as T

--import           Database.SQLite.Simple (SQLData(..))
import           Database.HDBC (SqlValue(..))

data FirebirdSyntax = FirebirdSyntax Builder (DL.DList SqlValue)

instance Monoid FirebirdSyntax where
  mempty = FirebirdSyntax mempty mempty
  mappend (FirebirdSyntax ab av) (FirebirdSyntax bb bv) =
    FirebirdSyntax (ab <> bb) (av <> bv)

instance Eq FirebirdSyntax where
  FirebirdSyntax ab av == FirebirdSyntax bb bv =
    toLazyByteString ab == toLazyByteString bb &&
    av == bv

emit :: ByteString -> FirebirdSyntax
emit b = FirebirdSyntax (byteString b) mempty

emit' :: Show a => a -> FirebirdSyntax
emit' x = FirebirdSyntax (byteString (fromString (show x))) mempty

quotedIdentifier :: T.Text -> FirebirdSyntax
quotedIdentifier txt = emit "\"" <> FirebirdSyntax (stringUtf8 (T.unpack (escape txt))) mempty <> emit "\""
  where escape = T.concatMap (\c -> if c == '"' then "\"\"" else T.singleton c)

emitValue :: SqlValue -> FirebirdSyntax
emitValue v = FirebirdSyntax (byteString "?") (DL.singleton v)

-- * Syntax types

newtype FirebirdCommandSyntax = FirebirdCommandSyntax { fromFirebirdCommand :: FirebirdSyntax }
newtype FirebirdSelectSyntax = FirebirdSelectSyntax { fromFirebirdSelect :: FirebirdSyntax }
newtype FirebirdInsertSyntax = FirebirdInsertSyntax { fromFirebirdInsert :: FirebirdSyntax }
newtype FirebirdUpdateSyntax = FirebirdUpdateSyntax { fromFirebirdUpdate :: FirebirdSyntax }
newtype FirebirdDeleteSyntax = FirebirdDeleteSyntax { fromFirebirdDelete :: FirebirdSyntax }

newtype FirebirdSelectTableSyntax = FirebirdSelectTableSyntax { fromFirebirdSelectTable :: FirebirdSyntax }
newtype FirebirdExpressionSyntax = FirebirdExpressionSyntax { fromFirebirdExpression :: FirebirdSyntax } deriving Eq
newtype FirebirdFromSyntax = FirebirdFromSyntax { fromFirebirdFromSyntax :: FirebirdSyntax }
newtype FirebirdComparisonQuantifierSyntax = FirebirdComparisonQuantifierSyntax { fromFirebirdComparisonQuantifier :: FirebirdSyntax }
newtype FirebirdExtractFieldSyntax = FirebirdExtractFieldSyntax { fromFirebirdExtractField :: FirebirdSyntax }
newtype FirebirdAggregationSetQuantifierSyntax = FirebirdAggregationSetQuantifierSyntax { fromFirebirdAggregationSetQuantifier :: FirebirdSyntax }
newtype FirebirdProjectionSyntax = FirebirdProjectionSyntax { fromFirebirdProjection :: FirebirdSyntax }
newtype FirebirdGroupingSyntax = FirebirdGroupingSyntax { fromFirebirdGrouping :: FirebirdSyntax }
newtype FirebirdOrderingSyntax = FirebirdOrderingSyntax { fromFirebirdOrdering :: FirebirdSyntax }
newtype FirebirdValueSyntax = FirebirdValueSyntax { fromFirebirdValue :: FirebirdSyntax }
newtype FirebirdTableSourceSyntax = FirebirdTableSourceSyntax { fromFirebirdTableSource :: FirebirdSyntax }
newtype FirebirdFieldNameSyntax = FirebirdFieldNameSyntax { fromFirebirdFieldNameSyntax :: FirebirdSyntax }
newtype FirebirdInsertValuesSyntax = FirebirdInsertValuesSyntax { fromFirebirdInsertValues :: FirebirdSyntax }
newtype FirebirdCreateTableSyntax = FirebirdCreateTableSyntax { fromFirebirdCreateTable :: FirebirdSyntax }
data FirebirdTableOptionsSyntax = FirebirdTableOptionsSyntax FirebirdSyntax FirebirdSyntax
newtype FirebirdColumnSchemaSyntax = FirebirdColumnSchemaSyntax { fromFirebirdColumnSchema :: FirebirdSyntax }
newtype FirebirdDataTypeSyntax = FirebirdDataTypeSyntax { fromFirebirdDataType :: FirebirdSyntax }
newtype FirebirdColumnConstraintDefinitionSyntax = FirebirdColumnConstraintDefinitionSyntax { fromFirebirdColumnConstraintDefinition :: FirebirdSyntax }
newtype FirebirdColumnConstraintSyntax = FirebirdColumnConstraintSyntax { fromFirebirdColumnConstraint :: FirebirdSyntax }
newtype FirebirdTableConstraintSyntax = FirebirdTableConstraintSyntax { fromFirebirdTableConstraint :: FirebirdSyntax }
newtype FirebirdMatchTypeSyntax = FirebirdMatchTypeSyntax { fromFirebirdMatchType :: FirebirdSyntax }
newtype FirebirdReferentialActionSyntax = FirebirdReferentialActionSyntax { fromFirebirdReferentialAction :: FirebirdSyntax }

instance IsSql92Syntax FirebirdCommandSyntax where
  type Sql92SelectSyntax FirebirdCommandSyntax = FirebirdSelectSyntax
  type Sql92InsertSyntax FirebirdCommandSyntax = FirebirdInsertSyntax
  type Sql92UpdateSyntax FirebirdCommandSyntax = FirebirdUpdateSyntax
  type Sql92DeleteSyntax FirebirdCommandSyntax = FirebirdDeleteSyntax

  selectCmd = FirebirdCommandSyntax . fromFirebirdSelect
  insertCmd = FirebirdCommandSyntax . fromFirebirdInsert
  updateCmd = FirebirdCommandSyntax . fromFirebirdUpdate
  deleteCmd = FirebirdCommandSyntax . fromFirebirdDelete

instance IsSql92SelectSyntax FirebirdSelectSyntax where
  type Sql92SelectSelectTableSyntax FirebirdSelectSyntax = FirebirdSelectTableSyntax
  type Sql92SelectOrderingSyntax FirebirdSelectSyntax = FirebirdOrderingSyntax

  selectStmt tbl ordering limit offset =
    FirebirdSelectSyntax $
    fromFirebirdSelectTable tbl <>
    (case ordering of
       [] -> mempty
       _ -> emit " ORDER BY " <> commas (coerce ordering)) <>
    maybe mempty ((emit " LIMIT " <>) . emit') limit <>
    maybe mempty ((emit " OFFSET " <>) . emit') offset

instance IsSql92SelectTableSyntax FirebirdSelectTableSyntax where
  type Sql92SelectTableSelectSyntax FirebirdSelectTableSyntax = FirebirdSelectSyntax
  type Sql92SelectTableExpressionSyntax FirebirdSelectTableSyntax = FirebirdExpressionSyntax
  type Sql92SelectTableProjectionSyntax FirebirdSelectTableSyntax = FirebirdProjectionSyntax
  type Sql92SelectTableFromSyntax FirebirdSelectTableSyntax = FirebirdFromSyntax
  type Sql92SelectTableGroupingSyntax FirebirdSelectTableSyntax = FirebirdGroupingSyntax

  selectTableStmt proj from where_ grouping having =
    FirebirdSelectTableSyntax $
    emit "SELECT " <> fromFirebirdProjection proj <>
    maybe mempty (emit " FROM " <>) (fromFirebirdFromSyntax <$> from) <>
    maybe mempty (emit " WHERE " <>) (fromFirebirdExpression <$> where_) <>
    maybe mempty (emit " GROUP BY " <>) (fromFirebirdGrouping <$> grouping) <>
    maybe mempty (emit " HAVING " <>) (fromFirebirdExpression <$> having)

  unionTables all = tableOp (if all then "UNION ALL" else "UNION")
  intersectTables all = tableOp (if all then "INTERSECT ALL" else "INTERSECT")
  exceptTable all = tableOp (if all then "EXCEPT ALL" else "EXCEPT")

tableOp :: ByteString -> FirebirdSelectTableSyntax -> FirebirdSelectTableSyntax -> FirebirdSelectTableSyntax
tableOp op a b =
  FirebirdSelectTableSyntax $
  parens (fromFirebirdSelectTable a) <> spaces (emit op) <> parens (fromFirebirdSelectTable b)

instance IsSql92FromSyntax FirebirdFromSyntax where
  type Sql92FromExpressionSyntax FirebirdFromSyntax = FirebirdExpressionSyntax
  type Sql92FromTableSourceSyntax FirebirdFromSyntax = FirebirdTableSourceSyntax

  fromTable tableSrc Nothing = FirebirdFromSyntax (fromFirebirdTableSource tableSrc)
  fromTable tableSrc (Just nm) =
    FirebirdFromSyntax (fromFirebirdTableSource tableSrc <> emit " AS " <> quotedIdentifier nm)

  innerJoin = _join "INNER JOIN"
  leftJoin = _join "LEFT JOIN"
  rightJoin = _join "RIGHT JOIN"

_join :: ByteString -> FirebirdFromSyntax -> FirebirdFromSyntax -> Maybe FirebirdExpressionSyntax -> FirebirdFromSyntax
_join joinType a b Nothing =
  FirebirdFromSyntax (fromFirebirdFromSyntax a <> spaces (emit joinType) <> fromFirebirdFromSyntax b)
_join joinType a b (Just on) =
  FirebirdFromSyntax (fromFirebirdFromSyntax a <> spaces (emit joinType) <> fromFirebirdFromSyntax b <> emit " ON " <> fromFirebirdExpression on)

instance IsSql92ProjectionSyntax FirebirdProjectionSyntax where
  type Sql92ProjectionExpressionSyntax FirebirdProjectionSyntax = FirebirdExpressionSyntax

  projExprs exprs =
    FirebirdProjectionSyntax $
    commas (map (\(expr, nm) -> fromFirebirdExpression expr <>
                                maybe mempty (\nm -> emit " AS " <> quotedIdentifier nm) nm) exprs)

instance IsSql92FieldNameSyntax FirebirdFieldNameSyntax where
  qualifiedField a b =
    FirebirdFieldNameSyntax $
    quotedIdentifier a <> emit "." <> quotedIdentifier b
  unqualifiedField a =
    FirebirdFieldNameSyntax $
    quotedIdentifier a

instance IsSql92TableSourceSyntax FirebirdTableSourceSyntax where
  type Sql92TableSourceSelectSyntax FirebirdTableSourceSyntax = FirebirdSelectSyntax

  tableNamed = FirebirdTableSourceSyntax . quotedIdentifier
  tableFromSubSelect s =
    FirebirdTableSourceSyntax (parens (fromFirebirdSelect s))

instance IsSql92GroupingSyntax FirebirdGroupingSyntax where
  type Sql92GroupingExpressionSyntax FirebirdGroupingSyntax = FirebirdExpressionSyntax

  groupByExpressions es =
    FirebirdGroupingSyntax $
    commas (map fromFirebirdExpression es)

instance IsSql92OrderingSyntax FirebirdOrderingSyntax where
  type Sql92OrderingExpressionSyntax FirebirdOrderingSyntax = FirebirdExpressionSyntax

  ascOrdering e = FirebirdOrderingSyntax (fromFirebirdExpression e <> emit " ASC")
  descOrdering e = FirebirdOrderingSyntax (fromFirebirdExpression e <> emit " DESC")

instance HasSqlValueSyntax FirebirdValueSyntax Int where
  sqlValueSyntax i = FirebirdValueSyntax (emitValue (SqlInteger (fromIntegral i)))

instance IsSql92ExpressionSyntax FirebirdExpressionSyntax where
  type Sql92ExpressionValueSyntax FirebirdExpressionSyntax = FirebirdValueSyntax
  type Sql92ExpressionSelectSyntax FirebirdExpressionSyntax = FirebirdSelectSyntax
  type Sql92ExpressionFieldNameSyntax FirebirdExpressionSyntax = FirebirdFieldNameSyntax
  type Sql92ExpressionQuantifierSyntax FirebirdExpressionSyntax = FirebirdComparisonQuantifierSyntax
  type Sql92ExpressionCastTargetSyntax FirebirdExpressionSyntax = FirebirdDataTypeSyntax
  type Sql92ExpressionExtractFieldSyntax FirebirdExpressionSyntax = FirebirdExtractFieldSyntax

  addE = binOp "+"; subE = binOp "-"; mulE = binOp "*"; divE = binOp "/"
  modE = binOp "%"; orE = binOp "OR"; andE = binOp "AND"; likeE = binOp "LIKE"
  overlapsE = binOp "OVERLAPS"

  eqE = compOp "="; neqE = compOp "<>"; ltE = compOp "<"; gtE = compOp ">"
  leE = compOp "<="; geE = compOp ">="

  negateE = unOp "-"; notE = unOp "NOT"

  isNotNullE = postFix "IS NOT NULL"; isNullE = postFix "IS NULL"
  isTrueE = postFix "IS TRUE"; isNotTrueE = postFix "IS NOT TRUE"
  isFalseE = postFix "IS FALSE"; isNotFalseE = postFix "IS NOT FALSE"
  isUnknownE = postFix "IS UNKNOWN"; isNotUnknownE = postFix "IS NOT UNKNOWN"

  existsE select = FirebirdExpressionSyntax (emit "EXISTS " <> parens (fromFirebirdSelect select))
  uniqueE select = FirebirdExpressionSyntax (emit "UNIQUE " <> parens (fromFirebirdSelect select))

  betweenE a b c = FirebirdExpressionSyntax (parens (fromFirebirdExpression a) <>
                                           emit " BETWEEN " <>
                                           parens (fromFirebirdExpression b) <>
                                           emit " AND " <>
                                           parens (fromFirebirdExpression c))

  valueE = FirebirdExpressionSyntax . fromFirebirdValue

  rowE vs = FirebirdExpressionSyntax (parens (commas (map fromFirebirdExpression vs)))
  fieldE = FirebirdExpressionSyntax . fromFirebirdFieldNameSyntax

  subqueryE = FirebirdExpressionSyntax . parens . fromFirebirdSelect

  positionE needle haystack =
    FirebirdExpressionSyntax $
    emit "POSITION" <> parens (parens (fromFirebirdExpression needle) <> emit " IN " <> parens (fromFirebirdExpression haystack))
  nullIfE a b =
    FirebirdExpressionSyntax $
    emit "NULLIF" <> parens (fromFirebirdExpression a <> emit ", " <> fromFirebirdExpression b)
  absE x = FirebirdExpressionSyntax (emit "ABS" <> parens (fromFirebirdExpression x))
  bitLengthE x = FirebirdExpressionSyntax (emit "BIT_LENGTH" <> parens (fromFirebirdExpression x))
  charLengthE x = FirebirdExpressionSyntax (emit "CHAR_LENGTH" <> parens (fromFirebirdExpression x))
  octetLengthE x = FirebirdExpressionSyntax (emit "OCTET_LENGTH" <> parens (fromFirebirdExpression x))
  coalesceE es = FirebirdExpressionSyntax (emit "COALESCE" <> parens (commas (map fromFirebirdExpression es)))
  extractE field from =
    FirebirdExpressionSyntax $
    emit "EXTRACT" <> parens (fromFirebirdExtractField field <> emit " FROM " <> parens (fromFirebirdExpression from))
  castE e t = FirebirdExpressionSyntax (emit "CAST" <> parens (parens (fromFirebirdExpression e) <> emit " TO " <> fromFirebirdDataType t))
  caseE cases else_ =
    FirebirdExpressionSyntax $
    emit "CASE " <>
    foldMap (\(cond, res) -> emit "WHEN " <> fromFirebirdExpression cond <> emit " THEN " <> fromFirebirdExpression res <> emit " ") cases <>
    emit "ELSE " <> fromFirebirdExpression else_ <> emit " END"

binOp :: ByteString -> FirebirdExpressionSyntax -> FirebirdExpressionSyntax -> FirebirdExpressionSyntax
binOp op a b =
  FirebirdExpressionSyntax $
  parens (fromFirebirdExpression a) <> emit " " <> emit op <> emit " " <> parens (fromFirebirdExpression b)

compOp :: ByteString -> Maybe FirebirdComparisonQuantifierSyntax
       -> FirebirdExpressionSyntax -> FirebirdExpressionSyntax
       -> FirebirdExpressionSyntax
compOp op quantifier a b =
  FirebirdExpressionSyntax $
  parens (fromFirebirdExpression a) <>
  emit op <>
  parens (maybe mempty (\q -> emit " " <> fromFirebirdComparisonQuantifier q <> emit " ") quantifier <>
          fromFirebirdExpression b)

unOp, postFix :: ByteString -> FirebirdExpressionSyntax -> FirebirdExpressionSyntax
unOp op a =
  FirebirdExpressionSyntax (emit op <> parens (fromFirebirdExpression a))
postFix op a =
  FirebirdExpressionSyntax (parens (fromFirebirdExpression a) <> emit " " <> emit op)

instance IsSql92AggregationExpressionSyntax FirebirdExpressionSyntax where
  type Sql92AggregationSetQuantifierSyntax FirebirdExpressionSyntax = FirebirdAggregationSetQuantifierSyntax

  countAllE = FirebirdExpressionSyntax (emit "COUNT(*)")
  countE = unAgg "COUNT"
  sumE = unAgg "SUM"
  avgE = unAgg "AVG"
  minE = unAgg "MIN"
  maxE = unAgg "MAX"

unAgg :: ByteString -> Maybe FirebirdAggregationSetQuantifierSyntax -> FirebirdExpressionSyntax
      -> FirebirdExpressionSyntax
unAgg fn q e =
  FirebirdExpressionSyntax $
  emit fn <> parens (maybe mempty (\q -> fromFirebirdAggregationSetQuantifier q <> emit " ") q <>
                     fromFirebirdExpression e)

instance IsSql92AggregationSetQuantifierSyntax FirebirdAggregationSetQuantifierSyntax where
  setQuantifierDistinct = FirebirdAggregationSetQuantifierSyntax (emit "DISTINCT")
  setQuantifierAll = FirebirdAggregationSetQuantifierSyntax (emit "ALL")

instance IsSql92InsertSyntax FirebirdInsertSyntax where
  type Sql92InsertValuesSyntax FirebirdInsertSyntax = FirebirdInsertValuesSyntax

  insertStmt tblName fields values =
    FirebirdInsertSyntax $
    emit "INSERT INTO " <> quotedIdentifier tblName <> parens (commas (map quotedIdentifier fields)) <>
    fromFirebirdInsertValues values

instance IsSql92InsertValuesSyntax FirebirdInsertValuesSyntax where
  type Sql92InsertValuesExpressionSyntax FirebirdInsertValuesSyntax = FirebirdExpressionSyntax
  type Sql92InsertValuesSelectSyntax FirebirdInsertValuesSyntax = FirebirdSelectSyntax

  insertSqlExpressions es =
    FirebirdInsertValuesSyntax $
    emit "VALUES " <>
    commas (map (parens . commas . map fromFirebirdExpression) es)
  insertFromSql (FirebirdSelectSyntax a) = FirebirdInsertValuesSyntax a

instance IsSql92UpdateSyntax FirebirdUpdateSyntax where
  type Sql92UpdateFieldNameSyntax FirebirdUpdateSyntax = FirebirdFieldNameSyntax
  type Sql92UpdateExpressionSyntax FirebirdUpdateSyntax = FirebirdExpressionSyntax

  updateStmt tbl fields where_ =
    FirebirdUpdateSyntax $
    emit "UPDATE " <> quotedIdentifier tbl <>
    (case fields of
       [] -> mempty
       _ -> emit " SET " <>
            commas (map (\(field, val) -> fromFirebirdFieldNameSyntax field <> emit "=" <> fromFirebirdExpression val) fields)) <>
    maybe mempty (\where_ -> emit " WHERE " <> fromFirebirdExpression where_) where_

instance IsSql92DeleteSyntax FirebirdDeleteSyntax where
  type Sql92DeleteExpressionSyntax FirebirdDeleteSyntax = FirebirdExpressionSyntax

  deleteStmt tbl where_ =
    FirebirdDeleteSyntax $
    emit "DELETE FROM " <> quotedIdentifier tbl <>
    maybe mempty (\where_ -> emit " WHERE " <> fromFirebirdExpression where_) where_

spaces, parens :: FirebirdSyntax -> FirebirdSyntax
spaces a = emit " " <> a <> emit " " 
parens a = emit "(" <> a <> emit ")"

commas :: [FirebirdSyntax] -> FirebirdSyntax
commas [] = mempty
commas [x] = x
commas (x:xs) = x <> foldMap (emit ", " <>) xs
