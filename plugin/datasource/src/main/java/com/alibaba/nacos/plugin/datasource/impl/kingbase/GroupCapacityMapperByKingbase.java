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

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.constants.FieldConstant;
import com.alibaba.nacos.plugin.datasource.mapper.GroupCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The kingbase implementation of GroupCapacityMapper.
 *
 * @author nacos team
 */
public class GroupCapacityMapperByKingbase extends AbstractMapperByKingbase implements GroupCapacityMapper {

    @Override
    public String getDataSource() {
        return DataSourceConstant.KINGBASE;
    }

    @Override
    public MapperResult selectGroupInfoBySize(MapperContext context) {
        String sql = "SELECT id, group_id FROM group_capacity WHERE id > ? LIMIT ? OFFSET ?";
        return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter(FieldConstant.ID),
                context.getPageSize(), context.getStartRow()));
    }

    @Override
    public MapperResult select(MapperContext context) {
        String sql = "SELECT id, quota, \"usage\", max_size, max_aggr_count, max_aggr_size, group_id FROM group_capacity "
                + "WHERE group_id = ?";
        return new MapperResult(sql, Collections.singletonList(context.getWhereParameter(FieldConstant.GROUP_ID)));
    }

    @Override
    public MapperResult insertIntoSelect(MapperContext context) {
        List<Object> paramList = Arrays.asList(
                context.getUpdateParameter(FieldConstant.GROUP_ID),
                context.getUpdateParameter(FieldConstant.QUOTA),
                context.getUpdateParameter(FieldConstant.MAX_SIZE),
                context.getUpdateParameter(FieldConstant.MAX_AGGR_COUNT),
                context.getUpdateParameter(FieldConstant.MAX_AGGR_SIZE),
                context.getUpdateParameter(FieldConstant.GMT_CREATE),
                context.getUpdateParameter(FieldConstant.GMT_MODIFIED)
        );

        String sql =
                "INSERT INTO group_capacity (group_id, quota, \"usage\", max_size, max_aggr_count, max_aggr_size,gmt_create,"
                + " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info";
        return new MapperResult(sql, paramList);
    }

    @Override
    public MapperResult insertIntoSelectByWhere(MapperContext context) {
        String sql =
                "INSERT INTO group_capacity (group_id, quota, \"usage\", max_size, max_aggr_count, max_aggr_size, gmt_create,"
                + " gmt_modified) SELECT ?, ?, count(*), ?, ?, ?, ?, ? FROM config_info WHERE group_id=? AND tenant_id = '"
                + NamespaceUtil.getNamespaceDefaultId() + "'";
        List<Object> paramList = Arrays.asList(
                context.getUpdateParameter(FieldConstant.GROUP_ID),
                context.getUpdateParameter(FieldConstant.QUOTA),
                context.getUpdateParameter(FieldConstant.MAX_SIZE),
                context.getUpdateParameter(FieldConstant.MAX_AGGR_COUNT),
                context.getUpdateParameter(FieldConstant.MAX_AGGR_SIZE),
                context.getUpdateParameter(FieldConstant.GMT_CREATE),
                context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
                context.getWhereParameter(FieldConstant.GROUP_ID)
        );
        return new MapperResult(sql, paramList);
    }

    @Override
    public MapperResult incrementUsageByWhereQuotaEqualZero(MapperContext context) {
        return new MapperResult(
                "UPDATE group_capacity SET \"usage\" = \"usage\" + 1, gmt_modified = ? WHERE group_id = ? AND \"usage\" < ? AND quota = 0",
                CollectionUtils.list(
                        context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
                        context.getWhereParameter(FieldConstant.GROUP_ID),
                        context.getWhereParameter(FieldConstant.USAGE)
                ));
    }

    @Override
    public MapperResult incrementUsageByWhereQuotaNotEqualZero(MapperContext context) {
        return new MapperResult(
                "UPDATE group_capacity SET \"usage\" = \"usage\" + 1, gmt_modified = ? WHERE group_id = ? AND \"usage\" < quota AND quota != 0",
                CollectionUtils.list(
                        context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
                        context.getWhereParameter(FieldConstant.GROUP_ID)
                ));
    }

    @Override
    public MapperResult incrementUsageByWhere(MapperContext context) {
        return new MapperResult(
                "UPDATE group_capacity SET \"usage\" = \"usage\" + 1, gmt_modified = ? WHERE group_id = ?",
                CollectionUtils.list(
                        context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
                        context.getWhereParameter(FieldConstant.GROUP_ID)
                ));
    }

    @Override
    public MapperResult decrementUsageByWhere(MapperContext context) {
        return new MapperResult(
                "UPDATE group_capacity SET \"usage\" = \"usage\" - 1, gmt_modified = ? WHERE group_id = ? AND \"usage\" > 0",
                CollectionUtils.list(
                        context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
                        context.getWhereParameter(FieldConstant.GROUP_ID)
                ));
    }

    @Override
    public MapperResult updateUsage(MapperContext context) {
        return new MapperResult(
                "UPDATE group_capacity SET \"usage\" = (SELECT count(*) FROM config_info), gmt_modified = ? WHERE group_id = ?",
                CollectionUtils.list(
                        context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
                        context.getWhereParameter(FieldConstant.GROUP_ID)
                ));
    }

    @Override
    public MapperResult updateUsageByWhere(MapperContext context) {
        return new MapperResult(
                "UPDATE group_capacity SET \"usage\" = (SELECT count(*) FROM config_info WHERE group_id=? AND tenant_id = '"
                + NamespaceUtil.getNamespaceDefaultId() + "')," + " gmt_modified = ? WHERE group_id= ?",
                CollectionUtils.list(
                        context.getWhereParameter(FieldConstant.GROUP_ID),
                        context.getUpdateParameter(FieldConstant.GMT_MODIFIED),
                        context.getWhereParameter(FieldConstant.GROUP_ID)
                ));
    }
}
