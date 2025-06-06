/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("query32") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    multi_sql """
    use ${db};
    set enable_nereids_planner=true;
    set enable_nereids_distribute_planner=false;
    set enable_fallback_to_original_planner=false;
    set exec_mem_limit=21G;
    set be_number_for_test=3;
    set enable_runtime_filter_prune=false;
    set parallel_pipeline_task_num=8;
    set forbid_unknown_col_stats=false;
    set enable_stats=true;
    set runtime_filter_type=8;
    set broadcast_row_count_limit = 30000000;
    set enable_nereids_timeout = false;
    set enable_pipeline_engine = true;
    set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
    set push_topn_to_agg = true;
    set topn_opt_limit_threshold=1024;
    """

    def ds = """select  sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   catalog_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 722
and i_item_sk = cs_item_sk 
and d_date between '2001-03-09' and 
        (cast('2001-03-09' as date) + interval 90 day)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '2001-03-09' and
                             (cast('2001-03-09' as date) + interval 90 day)
          and d_date_sk = cs_sold_date_sk 
      ) 
limit 100"""
    qt_ds_shape_32 '''
    explain shape plan
    select  
    /*+ leading(catalog_sales item date_dim) */
    sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   catalog_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 722
and i_item_sk = cs_item_sk 
and d_date between '2001-03-09' and 
        (cast('2001-03-09' as date) + interval 90 day)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '2001-03-09' and
                             (cast('2001-03-09' as date) + interval 90 day)
          and d_date_sk = cs_sold_date_sk 
      ) 
limit 100
    '''
}
