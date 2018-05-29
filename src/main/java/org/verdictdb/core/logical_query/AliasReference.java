package org.verdictdb.core.logical_query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javafx.util.Pair;

/**
 * Represents the alias name that appears in the group-by clause or in the order-by clause.
 * This column does not include any reference to the table.
 * 
 * @author Yongjoo Park
 *
 */
public class AliasReference implements GroupingAttribute {
    
    String aliasName;
    
    public AliasReference(String aliasName) {
        this.aliasName = aliasName;
    }
    
    public String getAliasName() {
        return aliasName;
    }
    
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}
