/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoBetaMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;

/**
 * The kingbase implementation of ConfigInfoBetaMapper.
 *
 * @author nacos team
 */
public class ConfigInfoBetaMapperByKingbase extends AbstractMapperByKingbase implements ConfigInfoBetaMapper {

    @Override
    public MapperResult findAllConfigInfoBetaForDumpAllFetchRows(MapperContext context) {
        int startRow = context.getStartRow();
        int pageSize = context.getPageSize();

        // Kingbase 使用 LIMIT offset, size 的分页语法，与 MySQL 兼容
        String sql = " SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,beta_ips,encrypted_data_key "
                + " FROM ( SELECT id FROM config_info_beta ORDER BY id LIMIT ? OFFSET ? ) g, config_info_beta t WHERE g.id = t.id ";

        List<Object> paramList = new ArrayList<>();
        paramList.add(pageSize);
        paramList.add(startRow);

        return new MapperResult(sql, paramList);
    }

    @Override
    public String getDataSource() {
        return DataSourceConstant.KINGBASE;
    }
}
