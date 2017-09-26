/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.relation.expr.SelectElem;

/**
 * Indicates that {@link SampleGroup#elems} can be computed by joining
 * {@link SampleGroup#sample}.
 * 
 * @author Yongjoo Park
 *
 */
public class SampleGroup {

    private ApproxRelation sample;

    private List<SelectElem> elems;

    // private double cost;

    /**
     * 
     * @param sample
     *            ApproxRelation instance
     * @param elems
     *            Expressions that can be answered using the sample.
     */
    public SampleGroup(ApproxRelation sample, List<SelectElem> elems) {
        this.sample = sample;
        this.elems = new ArrayList<SelectElem>();
        this.elems.addAll(elems);
    }

    public ApproxRelation getSample() {
        return sample;
    }

    public void setSample(ApproxRelation a) {
        sample = a;
    }

    public double samplingProb() {
        return sample.samplingProbability();
    }

    public double cost() {
        return sample.cost();
    }

    public String sampleType() {
        return sample.sampleType();
        // String type = null;
        // for (ApproxRelation param : samples) {
        // if (type == null) {
        // type = param.sampleType;
        // } else {
        // if (type.equals("uniform")) {
        // if (param.sampleType.equals("stratified")) {
        // type = "stratified";
        // } else {
        // type = "uniform";
        // }
        // } else if (type.equals("stratified")) {
        // type = "stratified";
        // } else if (type.equals("universe")) {
        // type = "universe";
        // }
        // }
        // }
        // return type;
    }

    public List<SelectElem> getElems() {
        return elems;
    }

    // public Set<SampleParam> sampleSet() {
    // return samples;
    // }

    @Override
    public String toString() {
        return elems.toString() + " =>\n" + sample.toString();
    }

    public boolean isEqualSample(SampleGroup o) {
        return sample.equals(o.getSample());
    }

    public void addElem(List<SelectElem> e) {
        elems.addAll(e);
    }

    // public Pair<Set<SampleParam>, List<Expr>> unroll() {
    // return Pair.of(samples, elems);
    // }

    public SampleGroup duplicate() {
        // Set<SampleParam> copiedSamples = new HashSet<SampleParam>(samples);
        List<SelectElem> copiedElems = new ArrayList<SelectElem>(elems);
        return new SampleGroup(sample, copiedElems);
    }
}
