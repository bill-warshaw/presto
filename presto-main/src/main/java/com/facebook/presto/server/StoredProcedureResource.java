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

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import java.util.List;

@Path("/v1/storedproc")
public class StoredProcedureResource
{
    private final StoredProcedureManager storedProcedureManager;

    @Inject
    public StoredProcedureResource(StoredProcedureManager storedProcedureManager)
    {
        this.storedProcedureManager = storedProcedureManager;
    }

    @GET
    public List<StoredProcedureInfo> getAllStoredProcedures()
    {
        return storedProcedureManager.getAll();
    }

    @GET
    @Path(("{storedProcName}"))
    public StoredProcedureInfo getStoredProcedure(@PathParam("storedProcName") String storedProcName)
    {
        return storedProcedureManager.get(storedProcName);
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public void createStoredProcedure(StoredProcedureInfo storedProcedureInfo)
    {
        storedProcedureManager.create(storedProcedureInfo);
    }

    @DELETE
    @Path(("{storedProcName}"))
    public void deleteStoredProcedure(@PathParam("storedProcName") String storedProcName)
    {
        storedProcedureManager.delete(storedProcName);
    }
}
