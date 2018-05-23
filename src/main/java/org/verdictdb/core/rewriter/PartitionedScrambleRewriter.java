package org.verdictdb.core.rewriter;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.logical_query.AbstractRelation;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.exception.UnexpectedTypeException;

public class PartitionedScrambleRewriter {
    
    SampleMeta sampleMeta;
    
    public PartitionedScrambleRewriter(SampleMeta sampleMeta) {
        this.sampleMeta = sampleMeta;
    }
    
    /**
     * Current Limitations:
     * 1. Only handles the query with a single aggregate (sub)query
     * 2. Only handles the query that the first select list is the aggregate query.
     * 
     * @param relation
     * @return
     * @throws UnexpectedTypeException 
     */
    AbstractRelation rewrite(AbstractRelation relation) throws UnexpectedTypeException {
        int partitionCount = sampleMeta.getPartitionCount();
        List<AbstractRelation> rewritten = new ArrayList<AbstractRelation>();
        
        for (int k = 1; k < partitionCount; k++) {
            rewritten.add(rewriteWithoutMaterialization(relation, k));
        }
        
        return relation;
    }
    
    AbstractRelation rewriteWithoutMaterialization(AbstractRelation relation, int partitionNumber) 
            throws UnexpectedTypeException {
        if (!(relation instanceof SelectQueryOp)) {
            throw new UnexpectedTypeException("Not implemented yet.");
        }
        
        SelectQueryOp sel = (SelectQueryOp) relation;
        
        
        // TODO
        return null;
    }

}
