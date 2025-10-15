package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.plugin.datasource.enums.kingbase.TrustedKingbaseFunctionEnum;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;

/**
 * 抽象类，提供 Kingbase 数据库相关的通用方法.
 *
 * @author nacos team
 */
public abstract class AbstractMapperByKingbase extends AbstractMapper {

    @Override
    public String getFunction(String functionName) {
        return TrustedKingbaseFunctionEnum.getFunctionByName(functionName);
    }
}
