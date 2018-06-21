/*
 * Copyright 2018 University of Michigan
 * 
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.rewriter.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.GroupingAttribute;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.core.rewriter.AliasRenamingRules;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.scramble.Scrambler;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;

/**
 * AQP rewriter for partitioned tables. A sampling probability column must exist.
 * 
 * @author Yongjoo Park
 *
 */
public class AggQueryRewriter {

  ScrambleMeta scrambleMeta;

  int nextAliasNumber = 1;

  public AggQueryRewriter(ScrambleMeta scrambleMeta) {
    this.scrambleMeta = scrambleMeta;
  }

  String generateNextAliasName() {
    String aliasName = "verdictdbalias" + nextAliasNumber;
    nextAliasNumber += 1;
    return aliasName;
  }

  void initializeAliasNameSequence() {
    nextAliasNumber = 1;
  }

  /**
   * 
   * Current Limitations:
   * 1. Only handles the query with a single aggregate (sub)query
   * 2. Only handles the query that the first select list is the aggregate query.
   * 
   * @param relation
   * @return
   * @throws VerdictDbException 
   */
  public List<AbstractRelation> rewrite(AbstractRelation relation) throws VerdictDbException {
    if (!(relation instanceof SelectQueryOp)) {
      throw new UnexpectedTypeException(relation);
    }
    else if (!relation.isAggregateQuery()) {
      throw new ValueException("The provided relation is not an aggregate relation.");
    }

    List<AbstractRelation> rewrittenQueries = rewriteAggregateQuery(relation);
    return rewrittenQueries;
  }

  public List<AbstractRelation> rewriteAggregateQuery(AbstractRelation relation) throws VerdictDbException {
    // propagate subsample ID and tier columns up toward the top sources.
    List<AbstractRelation> selectAllScrambledBases = new ArrayList<>();    // used for ingesting block agg predicate
    SelectQueryOp sel = (SelectQueryOp) relation;
    List<AbstractRelation> sourceList = sel.getFromList();
    sel.clearFromList();
    List<AbstractRelation> immediateScrambledSources = new ArrayList<>();
    Map<String, String> aliasUpdateMap = new HashMap<>();
    
    for (AbstractRelation source : sourceList) {
      AbstractRelation rewrittenSource = rewriteQueryRecursively(source, selectAllScrambledBases);
      sel.addTableSource(rewrittenSource);
      if (scrambleMeta.isScrambled(rewrittenSource.getAliasName().get())) {
        immediateScrambledSources.add(rewrittenSource);
      }
      aliasUpdateMap.put(source.getAliasName().get(), rewrittenSource.getAliasName().get());
    }
    
    // update aliases in the select list
    List<SelectItem> selectList = sel.getSelectList();
    sel.clearSelectList();
    for (SelectItem item : selectList) {
      sel.addSelectItem(replaceTableReferenceInSelectItem(item, aliasUpdateMap));
    }
    
    // update aliases in the groupby list
    List<GroupingAttribute> groupbyList = sel.getGroupby();
    sel.clearGroupby();
    for (GroupingAttribute group : groupbyList) {
      sel.addGroupby(replaceTableReferenceInGroupby(group, aliasUpdateMap));
    }

    List<AbstractRelation> rewrittenQueries = new ArrayList<>();
    if (immediateScrambledSources.size() == 0) {
      // no scrambled source exists
      rewrittenQueries.add(sel);
    }
    else {
      List<BaseColumn> baseColumns = new ArrayList<>();
      List<List<Pair<Integer, Integer>>> blockingIndices = new ArrayList<>();
      Pair<UnnamedColumn, UnnamedColumn> subsampleAndTierColumns = 
          planBlockAggregation(
              selectAllScrambledBases,
              immediateScrambledSources,
              baseColumns,
              blockingIndices);
      UnnamedColumn subsampleColumn = subsampleAndTierColumns.getLeft();
      UnnamedColumn tierColumn = subsampleAndTierColumns.getRight();
//              inclusionProbColumns);
      
      // rewrite aggregate functions with error estimation logic
      int savedNextAliasNumber = nextAliasNumber;
        for (int k = 0; k < blockingIndices.get(0).size(); k++) {
        nextAliasNumber = savedNextAliasNumber;
        AbstractRelation rewrittenQuery = 
            rewriteSelectListForErrorEstimation(sel, subsampleColumn, tierColumn);
        
        for (int m = 0; m < baseColumns.size(); m++) {
          BaseColumn blockaggBaseColumn = baseColumns.get(m);
          Pair<Integer, Integer> blockingIndex = blockingIndices.get(m).get(k);
          SelectQueryOp selectAllScramledBase = (SelectQueryOp) selectAllScrambledBases.get(m);
          selectAllScramledBase.clearFilters();
          selectAllScramledBase.addFilterByAnd(
              ColumnOp.equal(blockaggBaseColumn, ConstantColumn.valueOf(blockingIndex.getLeft())));
        }
        
        rewrittenQueries.add(deepcopySelectQuery((SelectQueryOp) rewrittenQuery));
      }
    }

    return rewrittenQueries;
  }
  
  SelectItem replaceTableReferenceInSelectItem(
      SelectItem oldColumn, Map<String, String> aliasUpdateMap)
          throws UnexpectedTypeException {
    if (oldColumn instanceof UnnamedColumn) {
      return replaceTableReferenceInUnnamedColumn((UnnamedColumn) oldColumn, aliasUpdateMap);
    }
    else if (oldColumn instanceof AliasedColumn) {
      AliasedColumn col = (AliasedColumn) oldColumn;
      return new AliasedColumn(
        replaceTableReferenceInUnnamedColumn(col.getColumn(), aliasUpdateMap),
        col.getAliasName());
    }
    else {
      throw new UnexpectedTypeException("Unexpected argument type: " + oldColumn.getClass().toString());
    }
  }
  
  UnnamedColumn replaceTableReferenceInUnnamedColumn(
      UnnamedColumn oldColumn, Map<String, String> aliasUpdateMap)
          throws UnexpectedTypeException {
    if (oldColumn instanceof BaseColumn) {
      BaseColumn col = (BaseColumn) oldColumn;
      BaseColumn newCol = new BaseColumn(aliasUpdateMap.get(
          col.getTableSourceAlias()), col.getColumnName());
      return newCol;
    }
    else if (oldColumn instanceof ColumnOp) {
        ColumnOp col = (ColumnOp) oldColumn;
        List<UnnamedColumn> newOperands = new ArrayList<>();
        for (UnnamedColumn c : col.getOperands()) {
          newOperands.add(replaceTableReferenceInUnnamedColumn(c, aliasUpdateMap));
        }
        return new ColumnOp(col.getOpType(), newOperands);
    }
    else {
      throw new UnexpectedTypeException("Unexpected argument type: " + oldColumn.getClass().toString());
    }
  }
  
  GroupingAttribute replaceTableReferenceInGroupby(
      GroupingAttribute oldGroup, Map<String, String> aliasUpdateMap) throws UnexpectedTypeException {
    if (oldGroup instanceof AliasReference) {
      return oldGroup;
    }
    else if (oldGroup instanceof UnnamedColumn) {
      return replaceTableReferenceInUnnamedColumn((UnnamedColumn) oldGroup, aliasUpdateMap);
    }
    else {
      throw new UnexpectedTypeException("Unexpected argument type: " + oldGroup.getClass().toString());
    }
  }
  
  
/**
 * 
 * @param selectAllScrambledBase    A list of select queries that include scrambled base tables (suppose length-m).
 * @param blockAggregateColumns    The columns of the scrambled tables that include the integers for block aggregates (length-m).
 * @param blockingIndices    The values to use for block aggregation (length-m).
 * @return A pair of sid column alias and tier column alias.
 * @throws ValueException
 */
  Pair<UnnamedColumn, UnnamedColumn> planBlockAggregation(
      List<AbstractRelation> selectAllScrambledBase,
      List<AbstractRelation> immediateScrambledSources,
      List<BaseColumn> blockAggregateColumns,
      List<List<Pair<Integer, Integer>>> blockingIndices)
//      List<UnnamedColumn> inclusionProbColumns)
          throws ValueException {
    
    if (selectAllScrambledBase.size() > 1) {
      // TODO: should support at least two in the future
      throw new ValueException("Only one scrambled table is expected.");
    }
    
    // block aggregation column and attribute values
    SelectQueryOp selectAllBase = (SelectQueryOp) selectAllScrambledBase.get(0);
    BaseTable scrambledBase = (BaseTable) selectAllBase.getFromList().get(0);
    String baseSchemaName = scrambledBase.getSchemaName();
    String baseTableName = scrambledBase.getTableName();
    int aggBlockCount = scrambleMeta.getAggregationBlockCount(baseSchemaName, baseTableName);
    String aggBlockColumn = scrambleMeta.getAggregationBlockColumn(baseSchemaName, baseTableName);
    blockAggregateColumns.add(new BaseColumn(scrambledBase.getAliasName().get(), aggBlockColumn));
    
    // for now, simply, a single fact table case.
    // TODO: we should support at least two fact tables.
    List<Pair<Integer, Integer>> blockingIndex = new ArrayList<>();
    for (int k = 0; k < aggBlockCount; k++) {
      blockingIndex.add(Pair.of(k,k));
    }
    blockingIndices.add(blockingIndex);
    
    AbstractRelation scrambledDirectSource = immediateScrambledSources.get(0);
    String scrambledSourceAliasName = scrambledDirectSource.getAliasName().get();
    
    // subsample column
    String subsampleColumnName = scrambleMeta.getSubsampleColumn(scrambledSourceAliasName);
    String tierColumnName = scrambleMeta.getTierColumn(scrambledSourceAliasName);
    return Pair.<UnnamedColumn, UnnamedColumn>of(
        new BaseColumn(scrambledSourceAliasName, subsampleColumnName),
        new BaseColumn(scrambledSourceAliasName, tierColumnName));
  }
  
  

  /**
   * Assuming the root is an aggregate query, rewriting performs the following.
   * 1. Recursively converts a select query (or a base table) into an alternative expression.
   * 2. If a base table is not a scrambled table, it returns the same base table.
   * 3. If a base table is a scrambled table, it performs:
   *    a. converts it into a select-star query (with a certain alias name).
   *    b. register the select-start query into the 'selectAllScrambled' list.
   *    c. register the information of the select-star query into 'scrambleMeta':
   *       (1) sid column
   *       (2) tier column
   * 4. If a select query is not a scrambled table, it returns the same select query.
   * 5. If a select query is a scrambled table, it performs:
   *    a. adds the following two columns to the select list and to 'scrambleMeta':
   *       (1) sid column
   *       (2) tier column
   *       
   * @param relation
   * @return
   * @throws VerdictDbException
   */
  public AbstractRelation rewriteQueryRecursively(
      AbstractRelation relation,
      List<AbstractRelation> selectAllScrambled)
          throws VerdictDbException {

    if (relation instanceof BaseTable) {
      BaseTable base = (BaseTable) relation;
      String baseTableAliasName = base.getAliasName().get();
      String baseSchemaName = base.getSchemaName();
      String baseTableName = base.getTableName();

      if (!scrambleMeta.isScrambled(baseSchemaName, baseTableName)) {
        return relation;
      }
      else {
        String newRelationAliasName = generateNextAliasName();
        SelectQueryOp sel = SelectQueryOp.getSelectQueryOp(Arrays.<SelectItem>asList(new AsteriskColumn()), base);
        sel.setAliasName(newRelationAliasName);

//        String inclusionProbabilityColumn =
//            scrambleMeta.getInclusionProbabilityColumn(baseSchemaName, baseTableName);
//        String inclusionProbBlockDiffColumn =
//            scrambleMeta.getInclusionProbabilityBlockDifferenceColumn(baseSchemaName, baseTableName);
        String subsampleColumn =
            scrambleMeta.getSubsampleColumn(baseSchemaName, baseTableName);
        String tierColumn = 
            scrambleMeta.getTierColumn(baseSchemaName, baseTableName);

        // new select list
//        String incProbColAliasName = generateNextAliasName();
//        String incProbBlockDiffAliasName = generateNextAliasName();
        String subsampleAliasName = generateNextAliasName();
        String tierAliasName = generateNextAliasName();

//        sel.addSelectItem(new AliasedColumn(
//            new BaseColumn(baseTableAliasName, inclusionProbabilityColumn), incProbColAliasName));
//        sel.addSelectItem(new AliasedColumn(
//            new BaseColumn(baseTableAliasName, inclusionProbBlockDiffColumn), incProbBlockDiffAliasName));
        sel.addSelectItem(new AliasedColumn(
            new BaseColumn(baseTableAliasName, subsampleColumn), subsampleAliasName));
        sel.addSelectItem(new AliasedColumn(
            new BaseColumn(baseTableAliasName, tierColumn), tierAliasName));

        // meta entry
        scrambleMeta.insertScrambleMetaEntry(
            newRelationAliasName,
//            incProbColAliasName,
//            incProbBlockDiffAliasName,
            subsampleAliasName,
            tierAliasName);

        selectAllScrambled.add(sel);
        return sel;
      }
    }
    else if (relation instanceof SelectQueryOp) {
      // call this method to every source relation (i.e., depth-first)
      SelectQueryOp sel = (SelectQueryOp) relation;
      List<AbstractRelation> oldSources = sel.getFromList();
      sel.clearFromList();
      boolean doesScrambledSourceExist = false;
      SelectQueryOp scrambledSource = null;      // assumes at most one table is a scrambled table.
      Map<String, String> aliasUpdateMap = new HashMap<>();
      
      for (AbstractRelation source : oldSources) {
        AbstractRelation rewrittenSource = rewriteQueryRecursively(source, selectAllScrambled);
        sel.addTableSource(rewrittenSource);
        if (scrambleMeta.isScrambled(rewrittenSource.getAliasName().get())) {
          doesScrambledSourceExist = true;
          scrambledSource = (SelectQueryOp) rewrittenSource;
        }
        aliasUpdateMap.put(source.getAliasName().get(), rewrittenSource.getAliasName().get());
      }
      
      // update aliases in the select list
      List<SelectItem> selectList = sel.getSelectList();
      sel.clearSelectList();
      for (SelectItem item : selectList) {
        sel.addSelectItem(replaceTableReferenceInSelectItem(item, aliasUpdateMap));
      }
      
      // update aliases in the groupby list
      List<GroupingAttribute> groupbyList = sel.getGroupby();
      sel.clearGroupby();
      for (GroupingAttribute group : groupbyList) {
        sel.addGroupby(replaceTableReferenceInGroupby(group, aliasUpdateMap));
      }

      if (!doesScrambledSourceExist) {
        return relation;
      }
      else {
        // insert meta columns to the select list of the current relation.
//        BaseTable scrambledTable = (BaseTable) ((SelectQueryOp) scrambledSource).getFromList().get(0);
//        String scrambledTableAliasName = scrambledTable.getAliasName().get();
//        String scrambledSchemaName = scrambledTable.getSchemaName();
//        String scrambledTableName = scrambledTable.getTableName();
        String scrambledSourceAliasName = scrambledSource.getAliasName().get();
//        String inclusionProbabilityColumn =
//            scrambleMeta.getInclusionProbabilityColumn(scrambledSourceAliasName);
//        String inclusionProbBlockDiffColumn =
//            scrambleMeta.getInclusionProbabilityBlockDifferenceColumn(scrambledSourceAliasName);
        String subsampleColumn =
            scrambleMeta.getSubsampleColumn(scrambledSourceAliasName);
        String tierColumn = 
            scrambleMeta.getTierColumn(scrambledSourceAliasName);

        // new select list
//        String incProbColAliasName = generateNextAliasName();
//        String incProbBlockDiffAliasName = generateNextAliasName();
        String subsampleAliasName = generateNextAliasName();
        String tierAliasName = generateNextAliasName();

//        sel.addSelectItem(new AliasedColumn(
//            new BaseColumn(scrambledSourceAliasName, inclusionProbabilityColumn), incProbColAliasName));
//        sel.addSelectItem(new AliasedColumn(
//            new BaseColumn(scrambledSourceAliasName, inclusionProbBlockDiffColumn), incProbBlockDiffAliasName));
        sel.addSelectItem(new AliasedColumn(
            new BaseColumn(scrambledSourceAliasName, subsampleColumn), subsampleAliasName));
        sel.addSelectItem(new AliasedColumn(
            new BaseColumn(scrambledSourceAliasName, tierColumn), tierAliasName));

        // meta entry
        scrambleMeta.insertScrambleMetaEntry(
            sel.getAliasName().get(),
//            incProbColAliasName,
//            incProbBlockDiffAliasName,
            subsampleAliasName,
            tierAliasName);

        return relation;
      }
    }
    else {
      throw new UnexpectedTypeException("An unexpected relation type: " + relation.getClass().toString());
    }
  }

  SelectQueryOp deepcopySelectQuery(SelectQueryOp relation) {
    SelectQueryOp sel = new SelectQueryOp();
    for (SelectItem c : relation.getSelectList()) {
      sel.addSelectItem(c);
    }
    
    for (AbstractRelation r : relation.getFromList()) {
      if (r instanceof SelectQueryOp) {
        sel.addTableSource(deepcopySelectQuery((SelectQueryOp) r));
      } else {
        sel.addTableSource(r);
      }
    }
    if (relation.getFilter().isPresent()) {
      sel.addFilterByAnd(relation.getFilter().get());
    }
    for (GroupingAttribute a : relation.getGroupby()) {
      sel.addGroupby(a);
    }
    if (relation.getAliasName().isPresent()) {
      sel.setAliasName(relation.getAliasName().get());
    }
    return sel;
  }

  /**
   * Rewrite a given query into AQP-enabled form. The rewritten queries do not include any "create table ..."
   * parts.
   * 
   * "select other_groups, sum(price) as alias from ... group by other_groups;" is converted to
   * "select tier, other_groups,
   *         sum(sub_sum_est) as sum_alias,
   *         sum(sub_sum_est * subsample_size) as sum_scaled_sum_alias,
   *         sum(sub_sum_est * sub_sum_est * subsample_size) as sum_square_scaled_sum_alias,
   *         count(*) as count_subsample,
   *         sum(subsample_size) as sum_subsample_size
   *  from (select tier, other_groups,
   *               sum(price) as sub_sum_est,
   *               sum(case 1 when price is not null else 0 end) as subsample_size
   *        from ...
   *        group by sid, tier, other_groups) as sub_table
   *  group by tier, other_groups;"
   *  
   *  
   * "select other_groups, count(*) as alias from ... group by other_groups;" is converted to
   * "select tier, other_groups,
   *         sum(subsample_size) as count_alias,
   *         sum(subsample_size * subsample_size) as sum_scaled_count_alias,
   *         sum(pow(subsample_size,3)) as sum_square_scaled_count_alias,
   *         count(*) as count_subsample,
   *         sum(subsample_size) as sum_subsample_size
   *  from (select tier, other_groups,
   *               count(*) as subsample_size
   *        from ...
   *        group by sid, tier, other_groups) as sub_table
   *  group by tier, other_groups;"
   *  
   *  
   * "select other_groups, avg(price) from ... group by other_groups;" is converted to
   * "select tier, other_groups,
   *         sum(sub_sum_est) as sum_alias,
   *         sum(sub_sum_est * subsample_size) as sum_scaled_sum_alias,
   *         sum(sub_sum_est * sub_sum_est * subsample_size) as sum_square_scaled_sum_alias,
   *         sum(subsample_size) as count_alias,
   *         sum(subsample_size * subsample_size) as sum_scaled_count_alias,
   *         sum(pow(subsample_size,3)) as sum_square_scaled_count_alias,
   *         count(*) as count_subsample,
   *         sum(subsample_size) as sum_subsample_size
   *  from (select tier, other_groups,
   *               sum(price) as sub_sum_est,
   *               sum(case 1 when price is not null else 0 end) as subsample_size
   *        from ...
   *        group by sid, tier, other_groups) as sub_table
   *  group by tier, other_groups;"
   *  
   *  This is based on the self-normalized estimator.
   *  https://statweb.stanford.edu/~owen/mc/Ch-var-is.pdf
   *  
   * @param relation
   * @param partitionNumber
   * @return
   * @throws VerdictDbException 
   */
  AbstractRelation rewriteSelectListForErrorEstimation(
      AbstractRelation relation,
      UnnamedColumn subsampleColumnOfSource,
      UnnamedColumn tierColumnOfSource)
//      UnnamedColumn inclusionProbabilityColumn)
          throws VerdictDbException {
    
    SelectQueryOp rewrittenOuter = new SelectQueryOp();
    SelectQueryOp rewrittenInner = new SelectQueryOp();
    String innerTableAliasName = generateNextAliasName();
    String innerTierColumnAliasName = generateNextAliasName();
    String outerTierColumnAliasName = AliasRenamingRules.tierAliasName();
    rewrittenInner.setAliasName(innerTableAliasName);
    SelectQueryOp sel = (SelectQueryOp) relation;
    List<SelectItem> selectList = sel.getSelectList();
    List<SelectItem> newInnerSelectList = new ArrayList<>();
    List<SelectItem> newOuterSelectList = new ArrayList<>();
    List<GroupingAttribute> groupbyList = sel.getGroupby();
    List<GroupingAttribute> newInnerGroupbyList = new ArrayList<>();
    List<GroupingAttribute> newOuterGroupbyList = new ArrayList<>();
    List<Pair<UnnamedColumn, Pair<String, String>>> innerNonaggregateSelectItemToAliases =
        new ArrayList<>();    // This pair is (innerAliasName, outerAliasName)
    
    // tier column
    newInnerSelectList.add(new AliasedColumn(tierColumnOfSource, innerTierColumnAliasName));
    newOuterSelectList.add(new AliasedColumn(
        new BaseColumn(innerTableAliasName, innerTierColumnAliasName),
        outerTierColumnAliasName));
    
    for (SelectItem item : selectList) {
      if (!(item instanceof AliasedColumn)) {
        throw new UnexpectedTypeException("The following select item is not aliased: " + item.toString());
      }

      UnnamedColumn c = ((AliasedColumn) item).getColumn();
      String aliasName = ((AliasedColumn) item).getAliasName();

      if (c instanceof BaseColumn) {
        String aliasForBase = generateNextAliasName();
        newInnerSelectList.add(new AliasedColumn(c, aliasForBase));
        newOuterSelectList.add(new AliasedColumn(new BaseColumn(innerTableAliasName, aliasForBase), aliasName));
        innerNonaggregateSelectItemToAliases.add(Pair.of(c, Pair.of(aliasForBase, aliasName)));
      }
      else if (c instanceof ColumnOp) {
        ColumnOp col = (ColumnOp) c;
        if (col.getOpType().equals("sum")) {
          String aliasForSubSumEst = generateNextAliasName();
          String aliasForSubsampleSize = generateNextAliasName();
          
          String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
          String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
          String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
          String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
          String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

          UnnamedColumn op = col.getOperand();        // argument within the sum function
//          ColumnOp newCol = ColumnOp.sum(ColumnOp.divide(op, inclusionProbabilityColumn));
          ColumnOp newCol = ColumnOp.sum(op);
          newInnerSelectList.add(new AliasedColumn(newCol, aliasForSubSumEst));   // aggregates of subsamples
          ColumnOp oneIfNotNull = ColumnOp.whenthenelse(
              ColumnOp.notnull(op),
              ConstantColumn.valueOf(1),
              ConstantColumn.valueOf(0));
          newInnerSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(oneIfNotNull),
                  aliasForSubsampleSize));      // size of each subsample
          
          // outer: for mean estimate
          newOuterSelectList.add(
              new AliasedColumn(ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubSumEst)),
                  aliasForSumEstimate));
          // outer: for error estimate
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(ColumnOp.multiply(
                      new BaseColumn(innerTableAliasName, aliasForSubSumEst),
                      new BaseColumn(innerTableAliasName, aliasForSubsampleSize))),
                  aliasForSumScaledSubsum));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(
                      ColumnOp.multiply(
                          ColumnOp.multiply(
                              new BaseColumn(innerTableAliasName, aliasForSubSumEst),
                              new BaseColumn(innerTableAliasName, aliasForSubSumEst)),
                          new BaseColumn(innerTableAliasName, aliasForSubsampleSize))),
                  aliasForSumSquaredScaledSubsum));
          newOuterSelectList.add(
              new AliasedColumn(ColumnOp.count(), aliasForCountSubsample));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubsampleSize)),
                  aliasForSumSubsampleSize));
        }
        else if (col.getOpType().equals("count")) {
//          String aliasForSubCountEst = generateNextAliasName();
          String aliasForSubsampleSize = generateNextAliasName();
          
          String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
          String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
          String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
          String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
          String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

          // inner
//          ColumnOp newCol = ColumnOp.sum(ColumnOp.divide(ConstantColumn.valueOf(1), inclusionProbabilityColumn));
//          newInnerSelectList.add(new AliasedColumn(newCol, aliasForSubCountEst));
          newInnerSelectList.add(
              new AliasedColumn(
                  ColumnOp.count(),
                  aliasForSubsampleSize));
          
          // outer: for mean estimate
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubsampleSize)),
                  aliasForCountEstimate));
          // outer: for standard dev estimate
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(ColumnOp.multiply(
                      new BaseColumn(innerTableAliasName, aliasForSubsampleSize),
                      new BaseColumn(innerTableAliasName, aliasForSubsampleSize))),
                  aliasForSumScaledSubcount));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(
                      ColumnOp.pow(
                          new BaseColumn(innerTableAliasName, aliasForSubsampleSize),
                          ConstantColumn.valueOf(3))),
                  aliasForSumSquaredScaledSubcount));
          newOuterSelectList.add(
              new AliasedColumn(ColumnOp.count(), aliasForCountSubsample));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubsampleSize)),
                  aliasForSumSubsampleSize));
        }
        else if (col.getOpType().equals("avg")) {
          String aliasForSubSumEst = generateNextAliasName();
//          String aliasForSubCountEst = generateNextAliasName();
          String aliasForSubsampleSize = generateNextAliasName();
          
          String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
          String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
          String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
          String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
          String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
          String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
          String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
          String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

          // inner
          UnnamedColumn op = col.getOperand();        // argument within the avg function
          ColumnOp newCol = ColumnOp.sum(op);
          newInnerSelectList.add(new AliasedColumn(newCol, aliasForSubSumEst));
          ColumnOp oneIfNotNull = ColumnOp.whenthenelse(
              ColumnOp.notnull(op),
              ConstantColumn.valueOf(1),
              ConstantColumn.valueOf(0));
//          newInnerSelectList.add(
//              new AliasedColumn(
//                  ColumnOp.sum(ColumnOp.divide(oneIfNotNull, inclusionProbabilityColumn)),
//                  aliasForSubCountEst));
          newInnerSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(oneIfNotNull),
                  aliasForSubsampleSize));
          
          // outer: mean estimate
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubSumEst)),
                  aliasForSumEstimate));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubsampleSize)),
                  aliasForCountEstimate));
          // outer: standard deviation estimate
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(ColumnOp.multiply(
                      new BaseColumn(innerTableAliasName, aliasForSubSumEst),
                      new BaseColumn(innerTableAliasName, aliasForSubsampleSize))),
                  aliasForSumScaledSubsum));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(
                      ColumnOp.multiply(
                          ColumnOp.multiply(
                              new BaseColumn(innerTableAliasName, aliasForSubSumEst),
                              new BaseColumn(innerTableAliasName, aliasForSubSumEst)),
                          new BaseColumn(innerTableAliasName, aliasForSubsampleSize))),
                  aliasForSumSquaredScaledSubsum));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(ColumnOp.multiply(
                      new BaseColumn(innerTableAliasName, aliasForSubsampleSize),
                      new BaseColumn(innerTableAliasName, aliasForSubsampleSize))),
                  aliasForSumScaledSubcount));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(
                      ColumnOp.pow(
                          new BaseColumn(innerTableAliasName, aliasForSubsampleSize),
                          ConstantColumn.valueOf(3))),
                  aliasForSumSquaredScaledSubcount));
          newOuterSelectList.add(
              new AliasedColumn(ColumnOp.count(), aliasForCountSubsample));
          newOuterSelectList.add(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(innerTableAliasName, aliasForSubsampleSize)),
                  aliasForSumSubsampleSize));
        }
        else {
          throw new UnexpectedTypeException("Not implemented yet.");
        }
      }
      else {
        throw new UnexpectedTypeException("Unexpected column type: " + c.getClass().toString());
      }
    }

    newInnerGroupbyList.add(subsampleColumnOfSource);
    newInnerGroupbyList.add(new AliasReference(innerTierColumnAliasName));
    newOuterGroupbyList.add(new AliasReference(outerTierColumnAliasName));
    
    for (GroupingAttribute a : groupbyList) {
      boolean added = false;
      for (Pair<UnnamedColumn, Pair<String, String>> pair : innerNonaggregateSelectItemToAliases) {
        UnnamedColumn col = pair.getLeft();
        String innerAlias = pair.getRight().getLeft();
        String outerAlias = pair.getRight().getRight();

        // when grouping attribute is a base column
        if (a.equals(col)) {
          newInnerGroupbyList.add(new AliasReference(innerAlias));
          newOuterGroupbyList.add(new AliasReference(outerAlias));
          added = true;
        }
        // when grouping attribute is the alias to an select item
        else if (a instanceof AliasReference && ((AliasReference) a).getAliasName().equals(outerAlias)) {
          newInnerGroupbyList.add(new AliasReference(innerAlias));
          newOuterGroupbyList.add(new AliasReference(outerAlias));
          added = true;
        }
      }
      if (added == false) {
        newInnerGroupbyList.add(a);
      }
    }

    // inner query
    for (SelectItem c : newInnerSelectList) {
      rewrittenInner.addSelectItem(c);
    }
    for (AbstractRelation r : sel.getFromList()) {
      rewrittenInner.addTableSource(r);
    }
    if (sel.getFilter().isPresent()) {
      rewrittenInner.addFilterByAnd(sel.getFilter().get());
    }
//    UnnamedColumn aggBlockPredicate = generateAggBlockPredicate(sel.getFromList(), aggblock);
//    rewrittenInner.addFilterByAnd(aggBlockPredicate);
    for (GroupingAttribute a : newInnerGroupbyList) {
      rewrittenInner.addGroupby(a);
    }

    // outer query
    for (SelectItem c : newOuterSelectList) {
      rewrittenOuter.addSelectItem(c);
    }
    rewrittenOuter.addTableSource(rewrittenInner);
    for (GroupingAttribute a : newOuterGroupbyList) {
      rewrittenOuter.addGroupby(a);
    }

    return rewrittenOuter;
  }

//  boolean isAggregateQuery(AbstractRelation relation) {
//    if (!(relation instanceof SelectQueryOp)) {
//      return false;
//    }
//    SelectQueryOp sel = (SelectQueryOp) relation;
//    List<SelectItem> fromList = sel.getSelectList();
//    for (SelectItem item : fromList) {
//      if (item instanceof ColumnOp) {
//        ColumnOp col = (ColumnOp) item;
//        if (isColumnOpAggregate(col)) {
//          return true;
//        }
//      }
//    }
//    return false;
//  }
//
//  boolean isColumnOpAggregate(ColumnOp col) {
//    if (col.getOpType().equals("avg") ||
//        col.getOpType().equals("sum") ||
//        col.getOpType().equals("count")) {
//      return true;
//    }
//    boolean aggExists = false;
//    List<UnnamedColumn> ops = col.getOperands();
//    for (UnnamedColumn c : ops) {
//      if (c instanceof ColumnOp) {
//        if (isColumnOpAggregate((ColumnOp) c)) {
//          aggExists = true;
//          break;
//        }
//      }
//    }
//    return aggExists;
//  }
}
