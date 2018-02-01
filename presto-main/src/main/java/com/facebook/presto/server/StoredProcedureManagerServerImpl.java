/*
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
package com.facebook.presto.server;

import com.facebook.presto.spi.StoredProcedureInfo;
import com.facebook.presto.spi.StoredProcedureManager;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StoredProcedureManagerServerImpl
        implements StoredProcedureManager
{
    private final Map<String, StoredProcedureInfo> storedProcedures;

    public StoredProcedureManagerServerImpl()
    {
        this.storedProcedures = new HashMap<>();
        storedProcedures.put("wat", new StoredProcedureInfo(
                "wat",
                Lists.newArrayList("a, b, c"),
                Lists.newArrayList(IntegerType.INTEGER, VarcharType.VARCHAR, BooleanType.BOOLEAN),
                "select * from tpch.nation"));
    }

    public List<StoredProcedureInfo> getAll()
    {
        return Lists.newArrayList(storedProcedures.values());
    }

    public StoredProcedureInfo get(String name)
    {
        return storedProcedures.get(name);
    }

    public void create(StoredProcedureInfo newStoredProcedure)
    {
        storedProcedures.put(newStoredProcedure.getName(), newStoredProcedure);
    }

    public void delete(String name)
    {
        storedProcedures.remove(name);
    }
}
