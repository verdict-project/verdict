/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.tpcds;

import edu.umich.verdict.TestBase;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

import java.io.FileNotFoundException;

public class TpcdsQuery68 {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        VerdictLogger.setLogLevel("debug");
        VerdictConf conf = new VerdictConf();
        conf.setDbms("hive2");
        conf.setHost("salat0.eecs.umich.edu");
        conf.setPort("10000");
        conf.setDbmsSchema("tpcds_bin_partitioned_orc_100");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select c_last_name ,c_first_name ,ca_city ,bought_city ,ss_ticket_number ,extended_price ,extended_tax ,list_price from (select ss_ticket_number ,ss_customer_sk ,ca_city bought_city ,sum(ss_ext_sales_price) extended_price ,sum(ss_ext_list_price) list_price ,sum(ss_ext_tax) extended_tax from store_sales ,date_dim, household_demographics, store, customer_address where store_sales.ss_sold_date_sk = date_dim.d_date_sk and store_sales.ss_store_sk = store.s_store_sk and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk and store_sales.ss_addr_sk = customer_address.ca_address_sk and date_dim.d_dom in (1,2) and (household_demographics.hd_dep_count = 4 or household_demographics.hd_vehicle_count= 2) and date_dim.d_year in (1998,1998+1,1998+2) and store.s_city in ('Fairview','Midway') group by ss_ticket_number ,ss_customer_sk ,ss_addr_sk,ca_city) dn ,customer ,customer_address current_addr where dn.ss_customer_sk = customer.c_customer_sk and customer.c_current_addr_sk = current_addr.ca_address_sk and current_addr.ca_city <> bought_city order by c_last_name ,ss_ticket_number limit 100;";
//        String sql = "select sum(ss_net_profit) from store_sales inner join store_returns on store_sales.ss_customer_sk = store_returns.sr_customer_sk;";
//        String sql = "select sum(ss_net_profit) from store_sales, store_returns where ss_customer_sk = sr_customer_sk;";
//        String sql = "select sum(ss_net_profit) from store_sales;";
//        String sql = "create 1% universe sample of store_sales on ss_store_sk, ss_item_sk";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }
}
