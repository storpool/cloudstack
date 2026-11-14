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
package org.apache.cloudstack.storage.datastore.driver;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.storage.ResizeVolumeAnswer;
import com.cloud.agent.api.storage.StorPoolResizeVolumeCommand;
import com.cloud.agent.api.storage.StorPoolSetVolumeEncryptionAnswer;
import com.cloud.agent.api.storage.StorPoolSetVolumeEncryptionCommand;
import com.cloud.agent.api.to.DataObjectType;
import com.cloud.exception.StorageUnavailableException;
import com.cloud.storage.DataStoreRole;
import com.cloud.storage.ResizeVolumePayload;
import com.cloud.storage.StorageManager;
import com.cloud.storage.StoragePool;
import com.cloud.storage.Storage.StoragePoolType;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.tags.dao.ResourceTagDao;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.VirtualMachine.State;
import com.cloud.vm.dao.VMInstanceDao;
import org.apache.cloudstack.engine.subsystem.api.storage.CopyCommandResult;
import org.apache.cloudstack.engine.subsystem.api.storage.CreateCmdResult;
import org.apache.cloudstack.engine.subsystem.api.storage.DataObject;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPoint;
import org.apache.cloudstack.engine.subsystem.api.storage.EndPointSelector;
import org.apache.cloudstack.engine.subsystem.api.storage.PrimaryDataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.framework.async.AsyncCompletionCallback;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailsDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.datastore.util.StorPoolUtil;
import org.apache.cloudstack.storage.to.PrimaryDataStoreTO;
import org.apache.cloudstack.storage.to.VolumeObjectTO;
import org.apache.cloudstack.storage.volume.VolumeObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;


@RunWith(MockitoJUnitRunner.class)
public class StorPoolPrimaryDataStoreDriverTest {

    @Mock
    private VMInstanceDao vmInstanceDao;

    @Mock
    private ResourceTagDao _resourceTagDao;

    @Mock
    private AsyncCompletionCallback<CopyCommandResult> callback;
    @Mock
    private PrimaryDataStoreDao storagePool;
    @Mock
    private StoragePoolDetailsDao detailsDao;
    @Mock
    private VolumeDao volumeDao;
    @Mock
    private StorageManager storageMgr;
    @Mock
    private AsyncCompletionCallback<CreateCmdResult> resizeCallback;
    @Mock
    private AsyncCompletionCallback<CreateCmdResult> createCallback;
    @Mock
    private EndPointSelector selector;

    private static final long GiB = 1024L * 1024 * 1024;
    private static final String VOLUME_PATH = "/dev/storpool-byid/t.t.t";
    private StoragePoolVO resizedPool;

    DataStore srcStore;
    DataStore destStore;

    DataObject srcObj;
    DataObject destObj;

    VolumeObjectTO srcTO;
    VolumeObjectTO dstTO;

    PrimaryDataStoreTO dstPrimaryTo;
    MockedStatic<StorPoolUtil> utilities;
    StorPoolUtil.SpConnectionDesc conn;

    @Before
    public void setUp(){
        utilities = Mockito.mockStatic(StorPoolUtil.class);
        conn = new StorPoolUtil.SpConnectionDesc("1.1.1.1:81", "123", "tmp");

        srcStore = mock(DataStore.class);
        destStore = mock(DataStore.class);

        srcObj = mock(VolumeInfo.class);
        destObj = mock(VolumeInfo.class);

        srcTO = mock(VolumeObjectTO.class);
        dstTO = mock(VolumeObjectTO.class);

        dstPrimaryTo = mock(PrimaryDataStoreTO.class);
    }

    @After
    public void tearDown(){
        utilities.close();
    }
    @InjectMocks
    private StorPoolPrimaryDataStoreDriver storPoolPrimaryDataStoreDriver;

    @Test
    public void testMigrateVolumePassed(){


        VMInstanceVO vm = mock(VMInstanceVO.class);
        setReturnsWhenSourceAndDestinationAreVolumes(srcStore, destStore, srcObj, destObj, srcTO, dstTO, dstPrimaryTo, vm);
        when(vm.getState()).thenReturn(State.Running);

        when(StorPoolUtil.getSpConnection(destObj.getDataStore().getUuid(), destObj.getDataStore().getId(), detailsDao, storagePool)).thenReturn(conn);
        StorPoolUtil.SpApiResponse resp = new StorPoolUtil.SpApiResponse();
        when(StorPoolUtil.volumeUpdateTemplate("~t.t.t", conn)).thenReturn(resp);

        when(volumeDao.findById(srcObj.getId())).thenReturn(mock(VolumeVO.class));
        storPoolPrimaryDataStoreDriver.copyAsync(srcObj, destObj, callback);
        utilities.verify(() -> StorPoolUtil.volumeUpdateTemplate("~t.t.t", conn), times(1));
        Assert.assertNull(resp.getError());
    }
    @Test
    public void testMigrateVolumeNotPassed() {

        VMInstanceVO vm = mock(VMInstanceVO.class);

        setReturnsWhenSourceAndDestinationAreVolumes(srcStore, destStore, srcObj, destObj, srcTO, dstTO, dstPrimaryTo, vm);
        when(vm.getState()).thenReturn(State.Running);

        when(StorPoolUtil.getSpConnection(destObj.getDataStore().getUuid(), destObj.getDataStore().getId(), detailsDao, storagePool)).thenReturn(conn);
        StorPoolUtil.SpApiResponse resp = new StorPoolUtil.SpApiResponse();
        setResponseError(resp);
        when(StorPoolUtil.volumeUpdateTemplate("~t.t.t", conn)).thenReturn(resp);
        storPoolPrimaryDataStoreDriver.copyAsync(srcObj, destObj, callback);
        Assert.assertNotNull(resp.getError());
    }

    @Test
    public void testCopyVolumeAttachedToVmPassed() {

        VMInstanceVO vm = mock(VMInstanceVO.class);

        setReturnsWhenSourceAndDestinationAreVolumes(srcStore, destStore, srcObj, destObj, srcTO, dstTO, dstPrimaryTo, vm);
        when(vm.getState()).thenReturn(State.Stopped);
        String vmUuid = UUID.randomUUID().toString();
        when(vm.getUuid()).thenReturn(vmUuid);

        when(StorPoolUtil.getSpConnection(destObj.getDataStore().getUuid(), destObj.getDataStore().getId(), detailsDao, storagePool)).thenReturn(conn);

        StorPoolUtil.SpApiResponse response = new StorPoolUtil.SpApiResponse();
        String volumeUuid = UUID.randomUUID().toString();
        when(srcObj.getUuid()).thenReturn(volumeUuid);
        when(StorPoolUtil.volumeCopy(volumeUuid, "~t.t.t", "volume", null, vmUuid, "", conn)).thenReturn(response);
        storPoolPrimaryDataStoreDriver.copyAsync(srcObj, destObj, callback);
        Assert.assertNull(response.getError());
    }

    @Test
    public void testCopyVolumeAttachedToVmNotPassed() {

        VMInstanceVO vm = mock(VMInstanceVO.class);

        setReturnsWhenSourceAndDestinationAreVolumes(srcStore, destStore, srcObj, destObj, srcTO, dstTO, dstPrimaryTo, vm);
        when(vm.getState()).thenReturn(State.Stopped);
        String vmUuid = UUID.randomUUID().toString();
        when(vm.getUuid()).thenReturn(vmUuid);

        when(StorPoolUtil.getSpConnection(destObj.getDataStore().getUuid(), destObj.getDataStore().getId(), detailsDao, storagePool)).thenReturn(conn);
        StorPoolUtil.SpApiResponse response = new StorPoolUtil.SpApiResponse();
        setResponseError(response);
        String volumeUuid = UUID.randomUUID().toString();
        when(srcObj.getUuid()).thenReturn(volumeUuid);
        when(StorPoolUtil.volumeCopy(volumeUuid, "~t.t.t", "volume", null, vmUuid, "", conn)).thenReturn(response);
        storPoolPrimaryDataStoreDriver.copyAsync(srcObj, destObj, callback);
        Assert.assertNotNull(response.getError());
    }
    @Test
    public void testCopyVolumeNotAttachedToVmNotPassed() {
        setReturnsWhenSourceAndDestinationAreVolumes(srcStore, destStore, srcObj, destObj, srcTO, dstTO, dstPrimaryTo, null);

        when(StorPoolUtil.getSpConnection(destObj.getDataStore().getUuid(), destObj.getDataStore().getId(), detailsDao, storagePool)).thenReturn(conn);
        StorPoolUtil.SpApiResponse response = new StorPoolUtil.SpApiResponse();
        setResponseError(response);
        String volumeUuid = UUID.randomUUID().toString();
        when(srcObj.getUuid()).thenReturn(volumeUuid);
        when(StorPoolUtil.volumeCopy(volumeUuid, "~t.t.t", "volume", null, null, null, conn)).thenReturn(response);
        storPoolPrimaryDataStoreDriver.copyAsync(srcObj, destObj, callback);
        utilities.verify(() -> StorPoolUtil.volumeCopy(volumeUuid, "~t.t.t", "volume", null, null, null, conn), times(1));

        Assert.assertNotNull(response.getError());
    }

    @Test
    public void testCopyVolumeNotAttachedToVmPassed() {

        setReturnsWhenSourceAndDestinationAreVolumes(srcStore, destStore, srcObj, destObj, srcTO, dstTO, dstPrimaryTo, null);

        when(StorPoolUtil.getSpConnection(destObj.getDataStore().getUuid(), destObj.getDataStore().getId(), detailsDao, storagePool)).thenReturn(conn);
        StorPoolUtil.SpApiResponse response = new StorPoolUtil.SpApiResponse();
        String volumeUuid = UUID.randomUUID().toString();
        when(srcObj.getUuid()).thenReturn(volumeUuid);
        when(StorPoolUtil.volumeCopy(volumeUuid, "~t.t.t", "volume", null, null, null, conn)).thenReturn(response);
        storPoolPrimaryDataStoreDriver.copyAsync(srcObj, destObj, callback);
        utilities.verify(() -> StorPoolUtil.volumeCopy(volumeUuid, "~t.t.t", "volume", null, null, null, conn), times(1));
        Assert.assertNull(response.getError());
    }

    @Test
    public void testResizeEncryptedVolumeAddsLuksHeaderToStorPoolSize() throws Exception {
        VolumeObject vol = mockVolumeForResize(10 * GiB, 20 * GiB, 1L);
        mockStorPoolVolumeUpdate(new StorPoolUtil.SpApiResponse());
        mockResizeAnswer(true);

        storPoolPrimaryDataStoreDriver.resize(vol, resizeCallback);

        verifyStorPoolVolumeUpdate(20 * GiB + StorPoolPrimaryDataStoreDriver.LUKS_HEADER_SIZE);
        StorPoolResizeVolumeCommand cmd = captureResizeCommand();
        Assert.assertEquals(10 * GiB, cmd.getCurrentSize());
        Assert.assertEquals(20 * GiB, cmd.getNewSize());
        verify(vol).setSize(20 * GiB);
        verify(resizedPool).setUsedBytes(10 * GiB);
        utilities.verify(() -> StorPoolUtil.volumeUpdate(any(String.class), anyLong(), eq(true), any(), any()), times(0));
    }

    @Test
    public void testResizeNotEncryptedVolumeUsesRequestedSize() throws Exception {
        VolumeObject vol = mockVolumeForResize(10 * GiB, 20 * GiB, null);
        mockStorPoolVolumeUpdate(new StorPoolUtil.SpApiResponse());
        mockResizeAnswer(true);

        storPoolPrimaryDataStoreDriver.resize(vol, resizeCallback);

        verifyStorPoolVolumeUpdate(20 * GiB);
        Assert.assertEquals(20 * GiB, captureResizeCommand().getNewSize());
        verify(vol).setSize(20 * GiB);
        verify(resizedPool).setUsedBytes(10 * GiB);
    }

    @Test
    public void testResizeEncryptedVolumeRollbackKeepsLuksHeader() throws Exception {
        VolumeObject vol = mockVolumeForResize(10 * GiB, 20 * GiB, 1L);
        mockStorPoolVolumeUpdate(new StorPoolUtil.SpApiResponse());
        mockResizeAnswer(false);
        when(StorPoolUtil.volumeUpdate(eq("~t.t.t"), anyLong(), eq(true), any(), eq(conn))).thenReturn(new StorPoolUtil.SpApiResponse());

        storPoolPrimaryDataStoreDriver.resize(vol, resizeCallback);

        utilities.verify(() -> StorPoolUtil.volumeUpdate("~t.t.t", 10 * GiB + StorPoolPrimaryDataStoreDriver.LUKS_HEADER_SIZE, true, null, conn), times(1));
        verify(vol, times(0)).setSize(anyLong());
    }

    @Test
    public void testResizeRollbackWhenAgentReturnsGenericAnswer() throws Exception {
        VolumeObject vol = mockVolumeForResize(10 * GiB, 20 * GiB, 1L);
        mockStorPoolVolumeUpdate(new StorPoolUtil.SpApiResponse());
        when(storageMgr.sendToPool(any(StoragePool.class), any(), any(StorPoolResizeVolumeCommand.class)))
                .thenReturn(new Answer(null, false, "Unable to detach volume"));
        when(StorPoolUtil.volumeUpdate(eq("~t.t.t"), anyLong(), eq(true), any(), eq(conn))).thenReturn(new StorPoolUtil.SpApiResponse());

        storPoolPrimaryDataStoreDriver.resize(vol, resizeCallback);

        utilities.verify(() -> StorPoolUtil.volumeUpdate("~t.t.t", 10 * GiB + StorPoolPrimaryDataStoreDriver.LUKS_HEADER_SIZE, true, null, conn), times(1));
        verify(vol, times(0)).setSize(anyLong());
    }

    @Test
    public void testResizeRollbackWhenSendingToAgentFails() throws Exception {
        VolumeObject vol = mockVolumeForResize(10 * GiB, 20 * GiB, 1L);
        mockStorPoolVolumeUpdate(new StorPoolUtil.SpApiResponse());
        when(storageMgr.sendToPool(any(StoragePool.class), any(), any(StorPoolResizeVolumeCommand.class)))
                .thenThrow(new StorageUnavailableException("No host is available", 1L));
        when(StorPoolUtil.volumeUpdate(eq("~t.t.t"), anyLong(), eq(true), any(), eq(conn))).thenReturn(new StorPoolUtil.SpApiResponse());

        storPoolPrimaryDataStoreDriver.resize(vol, resizeCallback);

        utilities.verify(() -> StorPoolUtil.volumeUpdate("~t.t.t", 10 * GiB + StorPoolPrimaryDataStoreDriver.LUKS_HEADER_SIZE, true, null, conn), times(1));
        verify(vol, times(0)).setSize(anyLong());
    }

    @Test
    public void testResizeNotEncryptedVolumeRollbackUsesOldSize() throws Exception {
        VolumeObject vol = mockVolumeForResize(10 * GiB, 20 * GiB, null);
        StorPoolUtil.SpApiResponse resp = new StorPoolUtil.SpApiResponse();
        setResponseError(resp);
        mockStorPoolVolumeUpdate(resp);
        when(StorPoolUtil.volumeUpdate(eq("~t.t.t"), anyLong(), eq(true), any(), eq(conn))).thenReturn(new StorPoolUtil.SpApiResponse());

        storPoolPrimaryDataStoreDriver.resize(vol, resizeCallback);

        utilities.verify(() -> StorPoolUtil.volumeUpdate("~t.t.t", 10 * GiB, true, null, conn), times(1));
        verify(storageMgr, times(0)).sendToPool(any(StoragePool.class), any(), any());
        verify(vol, times(0)).setSize(anyLong());
    }

    @Test
    public void testGetStorPoolVolumeSize() {
        VolumeInfo encrypted = mock(VolumeInfo.class);
        when(encrypted.getPassphraseId()).thenReturn(1L);
        VolumeInfo notEncrypted = mock(VolumeInfo.class);
        when(notEncrypted.getPassphraseId()).thenReturn(null);

        Assert.assertEquals(Long.valueOf(5 * GiB + StorPoolPrimaryDataStoreDriver.LUKS_HEADER_SIZE), StorPoolPrimaryDataStoreDriver.getStorPoolVolumeSize(encrypted, 5 * GiB));
        Assert.assertEquals(Long.valueOf(5 * GiB), StorPoolPrimaryDataStoreDriver.getStorPoolVolumeSize(notEncrypted, 5 * GiB));
        Assert.assertNull(StorPoolPrimaryDataStoreDriver.getStorPoolVolumeSize(encrypted, null));
    }

    @Test
    public void testCreateEncryptedVolumeSucceeds() {
        VolumeInfo vinfo = mockEncryptedVolumeForCreate();
        EndPoint ep = mock(EndPoint.class);
        when(selector.select(vinfo, true)).thenReturn(ep);
        when(ep.sendMessage(any(StorPoolSetVolumeEncryptionCommand.class))).thenReturn(new StorPoolSetVolumeEncryptionAnswer(new VolumeObjectTO()));

        storPoolPrimaryDataStoreDriver.createAsync(destStore, vinfo, createCallback);

        Assert.assertTrue(captureCreateResult().isSuccess());
        utilities.verify(() -> StorPoolUtil.volumeDelete(any(String.class), any()), times(0));
        verify(storagePool, times(1)).update(anyLong(), any(StoragePoolVO.class));
    }

    @Test
    public void testCreateEncryptedVolumeDeletesVolumeWhenEncryptionFails() {
        VolumeInfo vinfo = mockEncryptedVolumeForCreate();
        EndPoint ep = mock(EndPoint.class);
        when(selector.select(vinfo, true)).thenReturn(ep);
        when(ep.sendMessage(any(StorPoolSetVolumeEncryptionCommand.class))).thenReturn(new StorPoolSetVolumeEncryptionAnswer(null, false, "luksFormat failed"));

        storPoolPrimaryDataStoreDriver.createAsync(destStore, vinfo, createCallback);

        CreateCmdResult result = captureCreateResult();
        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals("luksFormat failed", result.getResult());
        utilities.verify(() -> StorPoolUtil.volumeDelete("~t.t.t", conn), times(1));
        verify(storagePool, times(0)).update(anyLong(), any(StoragePoolVO.class));
    }

    @Test
    public void testCreateEncryptedVolumeDeletesVolumeWhenAnswerIsNotEncryptionAnswer() {
        VolumeInfo vinfo = mockEncryptedVolumeForCreate();
        EndPoint ep = mock(EndPoint.class);
        when(selector.select(vinfo, true)).thenReturn(ep);
        when(ep.sendMessage(any(StorPoolSetVolumeEncryptionCommand.class))).thenReturn(new Answer(null, false, "host is not available"));

        storPoolPrimaryDataStoreDriver.createAsync(destStore, vinfo, createCallback);

        CreateCmdResult result = captureCreateResult();
        Assert.assertFalse(result.isSuccess());
        Assert.assertEquals("host is not available", result.getResult());
        utilities.verify(() -> StorPoolUtil.volumeDelete("~t.t.t", conn), times(1));
        verify(storagePool, times(0)).update(anyLong(), any(StoragePoolVO.class));
    }

    @Test
    public void testCreateEncryptedVolumeDeletesVolumeWhenNullAnswer() {
        VolumeInfo vinfo = mockEncryptedVolumeForCreate();
        EndPoint ep = mock(EndPoint.class);
        when(selector.select(vinfo, true)).thenReturn(ep);

        storPoolPrimaryDataStoreDriver.createAsync(destStore, vinfo, createCallback);

        Assert.assertFalse(captureCreateResult().isSuccess());
        utilities.verify(() -> StorPoolUtil.volumeDelete("~t.t.t", conn), times(1));
        verify(storagePool, times(0)).update(anyLong(), any(StoragePoolVO.class));
    }

    @Test
    public void testCreateEncryptedVolumeDeletesVolumeWhenNoHostSupportsEncryption() {
        VolumeInfo vinfo = mockEncryptedVolumeForCreate();
        when(selector.select(vinfo, true)).thenReturn(null);

        storPoolPrimaryDataStoreDriver.createAsync(destStore, vinfo, createCallback);

        Assert.assertFalse(captureCreateResult().isSuccess());
        utilities.verify(() -> StorPoolUtil.volumeDelete("~t.t.t", conn), times(1));
        verify(storagePool, times(0)).update(anyLong(), any(StoragePoolVO.class));
    }

    private VolumeInfo mockEncryptedVolumeForCreate() {
        VolumeInfo vinfo = mock(VolumeInfo.class);
        when(vinfo.getType()).thenReturn(DataObjectType.VOLUME);
        when(vinfo.getSize()).thenReturn(10 * GiB);
        when(vinfo.getPassphraseId()).thenReturn(1L);
        when(vinfo.getInstanceId()).thenReturn(null);
        when(vinfo.getDiskOfferingId()).thenReturn(null);
        when(vinfo.getMaxIops()).thenReturn(null);
        when(vinfo.getTO()).thenReturn(new VolumeObjectTO());
        when(destStore.getUuid()).thenReturn("SP_API_HTTP=1.1.1.1:81;SP_AUTH_TOKEN=token;SP_TEMPLATE=template_name");
        when(destStore.getId()).thenReturn(1L);
        when(StorPoolUtil.getSpConnection(destStore.getUuid(), destStore.getId(), detailsDao, storagePool)).thenReturn(conn);

        StorPoolUtil.SpApiResponse createResponse = new StorPoolUtil.SpApiResponse();
        when(StorPoolUtil.volumeCreate(any(), any(), any(), any(), any(), any(), any(), eq(conn))).thenReturn(createResponse);
        when(StorPoolUtil.getNameFromResponse(createResponse, false)).thenReturn("t.t.t");
        when(StorPoolUtil.devPath("t.t.t")).thenReturn(VOLUME_PATH);
        Mockito.lenient().when(StorPoolUtil.volumeDelete("~t.t.t", conn)).thenReturn(new StorPoolUtil.SpApiResponse());
        when(volumeDao.findById(anyLong())).thenReturn(mock(VolumeVO.class));
        Mockito.lenient().when(storagePool.findById(anyLong())).thenReturn(mock(StoragePoolVO.class));
        return vinfo;
    }

    private CreateCmdResult captureCreateResult() {
        ArgumentCaptor<CreateCmdResult> result = ArgumentCaptor.forClass(CreateCmdResult.class);
        verify(createCallback).complete(result.capture());
        return result.getValue();
    }

    private VolumeObject mockVolumeForResize(long oldSize, long newSize, Long passphraseId) {
        VolumeObject vol = mock(VolumeObject.class);
        PrimaryDataStore store = mock(PrimaryDataStore.class, withSettings().extraInterfaces(StoragePool.class));
        when(store.getUuid()).thenReturn("SP_API_HTTP=1.1.1.1:81;SP_AUTH_TOKEN=token;SP_TEMPLATE=template_name");
        when(store.getId()).thenReturn(1L);
        when(vol.getType()).thenReturn(DataObjectType.VOLUME);
        when(vol.getPath()).thenReturn(VOLUME_PATH);
        AtomicLong size = new AtomicLong(oldSize);
        when(vol.getSize()).thenAnswer(invocation -> size.get());
        Mockito.lenient().doAnswer(invocation -> {
            size.set(invocation.getArgument(0));
            return null;
        }).when(vol).setSize(anyLong());
        when(vol.getPassphraseId()).thenReturn(passphraseId);
        when(vol.getMaxIops()).thenReturn(null);
        when(vol.getDataStore()).thenReturn(store);
        when(vol.getpayload()).thenReturn(new ResizeVolumePayload(newSize, null, null, null, false, "i-2-3-VM", null, false));
        when(StorPoolUtil.getSpConnection(store.getUuid(), store.getId(), detailsDao, storagePool)).thenReturn(conn);
        resizedPool = mock(StoragePoolVO.class);
        Mockito.lenient().when(resizedPool.getCapacityBytes()).thenReturn(100 * GiB);
        Mockito.lenient().when(storagePool.findById(anyLong())).thenReturn(resizedPool);
        return vol;
    }

    private void mockStorPoolVolumeUpdate(StorPoolUtil.SpApiResponse resp) {
        when(StorPoolUtil.volumeUpdate(eq("~t.t.t"), anyLong(), eq(false), any(), eq(conn))).thenReturn(resp);
    }

    private void mockResizeAnswer(boolean result) throws Exception {
        ResizeVolumeAnswer answer = mock(ResizeVolumeAnswer.class);
        when(answer.getResult()).thenReturn(result);
        if (!result) {
            when(answer.getDetails()).thenReturn("Cannot grow device files");
        }
        when(storageMgr.sendToPool(any(StoragePool.class), any(), any(StorPoolResizeVolumeCommand.class))).thenReturn(answer);
    }

    private void verifyStorPoolVolumeUpdate(long size) {
        utilities.verify(() -> StorPoolUtil.volumeUpdate("~t.t.t", size, false, 0L, conn), times(1));
    }

    private StorPoolResizeVolumeCommand captureResizeCommand() throws Exception {
        ArgumentCaptor<StorPoolResizeVolumeCommand> captor = ArgumentCaptor.forClass(StorPoolResizeVolumeCommand.class);
        verify(storageMgr).sendToPool(any(StoragePool.class), any(), captor.capture());
        return captor.getValue();
    }

    private void setReturnsWhenSourceAndDestinationAreVolumes(DataStore srcStore, DataStore destStore, DataObject srcObj, DataObject destObj, VolumeObjectTO srcTO, VolumeObjectTO dstTO, PrimaryDataStoreTO dstPrimaryTo, VMInstanceVO vm) {
        when(srcStore.getRole()).thenReturn(DataStoreRole.Primary);
        when(destStore.getRole()).thenReturn(DataStoreRole.Primary);
        when(srcObj.getDataStore()).thenReturn(srcStore);
        when(destObj.getDataStore()).thenReturn(destStore);
        when(srcObj.getType()).thenReturn(DataObjectType.VOLUME);
        when(destObj.getType()).thenReturn(DataObjectType.VOLUME);
        when(destObj.getTO()).thenReturn(dstTO);
        when(srcObj.getTO()).thenReturn(srcTO);

        when(srcObj.getDataStore().getDriver()).thenReturn(storPoolPrimaryDataStoreDriver);
        when(destObj.getTO().getDataStore()).thenReturn(dstPrimaryTo);
        when(destObj.getDataStore().getUuid()).thenReturn("SP_API_HTTP=1.1.1.1:81;SP_AUTH_TOKEN=token;SP_TEMPLATE=template_name");
        when(destObj.getDataStore().getId()).thenReturn(1L);

        when(srcTO.getPath()).thenReturn("/dev/storpool-byid/t.t.t");
        when(dstPrimaryTo.getPoolType()).thenReturn(StoragePoolType.StorPool);
        when(vmInstanceDao.findById(anyLong())).thenReturn(vm);
    }

    private static void setResponseError(StorPoolUtil.SpApiResponse resp) {
        StorPoolUtil.SpApiError respErr = new StorPoolUtil.SpApiError();
        respErr.setName("error");
        respErr.setDescr("Failed");
        resp.setError(respErr);
    }
}
