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
package com.facebook.presto.spi;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class StoredProcedureInfo
{
    private final String name;
    private final List<String> outputColumnNames;
    private final List<Type> outputColumnTypes;
    private final String nativeQuery;

    @JsonCreator
    public StoredProcedureInfo(
            @JsonProperty("name") String name,
            @JsonProperty("outputColumnNames") List<String> outputColumnNames,
            @JsonProperty("outputColumnTypes") List<Type> outputColumnTypes,
            @JsonProperty("nativeQuery") String nativeQuery)
    {
        this.name = requireNonNull(name, "name is null");
        this.outputColumnNames = requireNonNull(outputColumnNames, "outputColumnNames is null");
        this.outputColumnTypes = requireNonNull(outputColumnTypes, "outputColumnTypes is null");
        this.nativeQuery = requireNonNull(nativeQuery, "nativeQuery is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<String> getOutputColumnNames()
    {
        return outputColumnNames;
    }

    @JsonProperty
    public List<Type> getOutputColumnTypes()
    {
        return outputColumnTypes;
    }

    @JsonProperty
    public String getNativeQuery()
    {
        return nativeQuery;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        StoredProcedureInfo o = (StoredProcedureInfo) obj;
        return Objects.equals(this.name, o.name) &&
                Objects.equals(this.outputColumnNames, o.outputColumnNames) &&
                Objects.equals(this.outputColumnTypes, o.outputColumnTypes) &&
                Objects.equals(this.nativeQuery, o.nativeQuery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, outputColumnNames, outputColumnTypes, nativeQuery);
    }

    @Override
    public String toString()
    {
        return String.format("[name:%s, outputColumnNames:%s, outputColumnTypes:%s, nativeQuery:%s]", name, outputColumnNames, outputColumnTypes, nativeQuery);
    }
}
