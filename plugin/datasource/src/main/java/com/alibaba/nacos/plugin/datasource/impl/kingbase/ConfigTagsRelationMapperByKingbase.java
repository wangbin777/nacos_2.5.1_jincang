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

import com.alibaba.nacos.common.utils.ArrayUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigTagsRelationMapper;
import com.alibaba.nacos.plugin.datasource.mapper.ext.WhereBuilder;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.ArrayList;
import java.util.List;

/**
 * The kingbase implementation of ConfigTagsRelationMapper.
 *
 * @author nacos team
 */
public class ConfigTagsRelationMapperByKingbase extends AbstractMapperByKingbase implements ConfigTagsRelationMapper {

    @Override
    public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
        final String[] tagArr = (String[]) context.getWhereParameter(FieldConstant.TAG_ARR);

        List<Object> paramList = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE ");
        final String sql =
                "SELECT a.id,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content FROM config_info a LEFT JOIN "
                        + "config_tags_relation b ON a.id=b.id";

        where.append(" a.tenant_id = ? ");
        paramList.add(tenant);

        if (StringUtils.isNotBlank(dataId)) {
            where.append(" AND a.data_id = ? ");
            paramList.add(dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            where.append(" AND a.group_id = ? ");
            paramList.add(group);
        }
        if (StringUtils.isNotBlank(appName)) {
            where.append(" AND a.app_name = ? ");
            paramList.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND a.content LIKE ? ");
            paramList.add(content);
        }

        if (tagArr != null && tagArr.length > 0) {
            where.append(" AND b.tag_name IN (");
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0) {
                    where.append(", ");
                }
                where.append('?');
                paramList.add(tagArr[i]);
            }
            where.append(") ");
        }

        where.append(" LIMIT ? OFFSET ? ");
        paramList.add(context.getPageSize());
        paramList.add(context.getStartRow());

        return new MapperResult(sql + where, paramList);
    }

    @Override
    public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
        final String[] tagArr = (String[]) context.getWhereParameter(FieldConstant.TAG_ARR);
        final String[] types = (String[]) context.getWhereParameter(FieldConstant.TYPE);

        WhereBuilder where = new WhereBuilder(
                "SELECT a.id,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content,a.type "
                        + "FROM config_info a LEFT JOIN config_tags_relation b ON a.id=b.id");

        where.like("a.tenant_id", tenant);

        if (StringUtils.isNotBlank(dataId)) {
            where.and().like("a.data_id", dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            where.and().like("a.group_id", group);
        }
        if (StringUtils.isNotBlank(appName)) {
            where.and().eq("a.app_name", appName);
        }
        if (StringUtils.isNotBlank(content)) {
            where.and().like("a.content", content);
        }
        if (!ArrayUtils.isEmpty(tagArr)) {
            where.and().in("b.tag_name", tagArr);
        }
        if (!ArrayUtils.isEmpty(types)) {
            where.and().in("a.type", types);
        }

        where.limit(context.getStartRow(), context.getPageSize());

        return where.build();
    }

    @Override
    public String getDataSource() {
        return DataSourceConstant.KINGBASE;
    }
}