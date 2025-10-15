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
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.constants.ContextConstant;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.mapper.ext.WhereBuilder;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * The kingbase implementation of ConfigInfoMapper.
 *
 * @author nacos team
 */
public class ConfigInfoMapperByKingbase extends AbstractMapperByKingbase implements ConfigInfoMapper {

    private static final String DATA_ID = "dataId";

    private static final String GROUP = "group";

    private static final String APP_NAME = "appName";

    private static final String CONTENT = "content";

    private static final String TENANT = "tenant";

    @Override
    public MapperResult findConfigInfoByAppFetchRows(MapperContext context) {
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String tenantId = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content FROM config_info"
                + " WHERE tenant_id LIKE ? AND app_name = ? LIMIT ? OFFSET ?";
        List<Object> params = CollectionUtils.list(tenantId, appName, context.getPageSize(), context.getStartRow());
        return new MapperResult(sql, params);
    }

    @Override
    public MapperResult getTenantIdList(MapperContext context) {
        String sql = "SELECT tenant_id FROM config_info WHERE tenant_id != '" + NamespaceUtil.getNamespaceDefaultId()
                + "' GROUP BY tenant_id LIMIT ? OFFSET ?";
        return new MapperResult(sql, CollectionUtils.list(context.getPageSize(), context.getStartRow()));
    }

    @Override
    public MapperResult getGroupIdList(MapperContext context) {
        String sql = "SELECT group_id FROM config_info WHERE tenant_id ='" + NamespaceUtil.getNamespaceDefaultId()
                + "' GROUP BY group_id LIMIT ? OFFSET ?";
        return new MapperResult(sql, CollectionUtils.list(context.getPageSize(), context.getStartRow()));
    }

    @Override
    public MapperResult findAllConfigKey(MapperContext context) {
        String sql = " SELECT data_id,group_id,app_name FROM ( "
                + " SELECT id FROM config_info WHERE tenant_id LIKE ? ORDER BY id LIMIT ? OFFSET ? ) g, config_info t WHERE g.id = t.id ";
        return new MapperResult(sql,
                CollectionUtils.list(context.getWhereParameter(FieldConstant.TENANT_ID), context.getPageSize(),
                        context.getStartRow()));
    }

    @Override
    public MapperResult findAllConfigInfoBaseFetchRows(MapperContext context) {
        String sql = "SELECT t.id,data_id,group_id,content,md5 FROM ( SELECT id FROM config_info ORDER BY id LIMIT ? OFFSET ? ) "
                + "g, config_info t WHERE g.id = t.id";
        return new MapperResult(sql, CollectionUtils.list(context.getPageSize(), context.getStartRow()));
    }

    @Override
    public MapperResult findAllConfigInfoFragment(MapperContext context) {
        boolean needContent = Boolean.parseBoolean(
                (String) context.getContextParameter(ContextConstant.NEED_CONTENT));
        String sql = "SELECT id,data_id,group_id,tenant_id,app_name," + (needContent ? "content," : "")
                + "md5,gmt_modified,type,encrypted_data_key FROM config_info WHERE id > ? ORDER BY id ASC LIMIT ? OFFSET ?";
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID),
                context.getPageSize(), context.getStartRow()));
    }

    @Override
    public MapperResult findChangeConfigFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final Timestamp startTime = (Timestamp) context.getWhereParameter(FieldConstant.START_TIME);
        final Timestamp endTime = (Timestamp) context.getWhereParameter(FieldConstant.END_TIME);

        List<Object> paramList = new ArrayList<>();
        StringBuilder where = new StringBuilder(" WHERE 1=1 ");
        if (!StringUtils.isBlank(dataId)) {
            where.append(" AND data_id LIKE ? ");
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where.append(" AND group_id LIKE ? ");
            paramList.add(group);
        }
        if (!StringUtils.isBlank(tenant)) {
            where.append(" AND tenant_id = ? ");
            paramList.add(tenant);
        }
        if (!StringUtils.isBlank(appName)) {
            where.append(" AND app_name = ? ");
            paramList.add(appName);
        }
        if (startTime != null) {
            where.append(" AND gmt_modified >= ? ");
            paramList.add(startTime);
        }
        if (endTime != null) {
            where.append(" AND gmt_modified <= ? ");
            paramList.add(endTime);
        }

        String lastMaxId = (String) context.getWhereParameter(FieldConstant.LAST_MAX_ID);
        paramList.add(lastMaxId);
        paramList.add(context.getPageSize());
        paramList.add(context.getStartRow());
        String sql = "SELECT id,data_id,group_id,tenant_id,app_name,type,md5,gmt_modified FROM config_info"
                + where.toString() + " AND id > ? ORDER BY id ASC LIMIT ? OFFSET ?";
        return new MapperResult(sql, paramList);
    }

    @Override
    public MapperResult listGroupKeyMd5ByPageFetchRows(MapperContext context) {
        String sql = "SELECT t.id,data_id,group_id,tenant_id,app_name,md5,type,gmt_modified,encrypted_data_key FROM "
                + "( SELECT id FROM config_info ORDER BY id LIMIT ? OFFSET ? ) g, config_info t WHERE g.id = t.id";
        return new MapperResult(sql, CollectionUtils.list(context.getPageSize(), context.getStartRow()));
    }

    @Override
    public MapperResult findConfigInfoBaseLikeFetchRows(MapperContext context) {
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

        StringBuilder where = new StringBuilder(" WHERE tenant_id='" + NamespaceUtil.getNamespaceDefaultId() + "'");
        List<Object> paramList = new ArrayList<>();

        if (!StringUtils.isBlank(dataId)) {
            where.append(" AND data_id LIKE ? ");
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where.append(" AND group_id LIKE ? ");
            paramList.add(group);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND content LIKE ? ");
            paramList.add(content);
        }

        String sql = "SELECT id,data_id,group_id,tenant_id,content FROM config_info" + where.toString()
                + " LIMIT ? OFFSET ?";
        paramList.add(context.getPageSize());
        paramList.add(context.getStartRow());

        return new MapperResult(sql, paramList);
    }

    @Override
    public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);

        StringBuilder where = new StringBuilder(" WHERE tenant_id = ? ");
        List<Object> paramList = new ArrayList<>();
        paramList.add(tenant);

        if (!StringUtils.isBlank(dataId)) {
            where.append(" AND data_id = ? ");
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where.append(" AND group_id = ? ");
            paramList.add(group);
        }
        if (!StringUtils.isBlank(appName)) {
            where.append(" AND app_name = ? ");
            paramList.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND content LIKE ? ");
            paramList.add(content);
        }

        String sql = "SELECT id,data_id,group_id,tenant_id,app_name,content,type,encrypted_data_key FROM config_info"
                + where.toString() + " LIMIT ? OFFSET ?";
        paramList.add(context.getPageSize());
        paramList.add(context.getStartRow());

        return new MapperResult(sql, paramList);
    }

    @Override
    public MapperResult findConfigInfoBaseByGroupFetchRows(MapperContext context) {
        String sql = "SELECT id,data_id,group_id,content FROM config_info WHERE group_id = ? AND tenant_id = ? LIMIT ? OFFSET ?";
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.GROUP_ID),
                context.getWhereParameter(FieldConstant.TENANT_ID), context.getPageSize(), context.getStartRow()));
    }

    @Override
    public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {
        final String tenant = (String) context.getWhereParameter(FieldConstant.TENANT_ID);
        final String dataId = (String) context.getWhereParameter(FieldConstant.DATA_ID);
        final String group = (String) context.getWhereParameter(FieldConstant.GROUP_ID);
        final String appName = (String) context.getWhereParameter(FieldConstant.APP_NAME);
        final String content = (String) context.getWhereParameter(FieldConstant.CONTENT);
        final String[] types = (String[]) context.getWhereParameter(FieldConstant.TYPE);

        WhereBuilder where = new WhereBuilder(
                "SELECT id,data_id,group_id,tenant_id,app_name,content,encrypted_data_key,type FROM config_info");
        where.like("tenant_id", tenant);

        if (StringUtils.isNotBlank(dataId)) {
            where.and().like("data_id", dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            where.and().like("group_id", group);
        }
        if (StringUtils.isNotBlank(appName)) {
            where.and().eq("app_name", appName);
        }
        if (StringUtils.isNotBlank(content)) {
            where.and().like("content", content);
        }
        if (!ArrayUtils.isEmpty(types)) {
            where.and().in("type", types);
        }

        where.limit(context.getStartRow(), context.getPageSize());

        return where.build();
    }

    @Override
    public MapperResult findAllConfigInfoFetchRows(MapperContext context) {
        String sql = "SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5 FROM ( SELECT id FROM config_info "
                + "WHERE tenant_id LIKE ? ORDER BY id LIMIT ? OFFSET ? ) g, config_info t WHERE g.id = t.id";
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.TENANT_ID),
                context.getPageSize(), context.getStartRow()));
    }

    @Override
    public String getDataSource() {
        return DataSourceConstant.KINGBASE;
    }
}
