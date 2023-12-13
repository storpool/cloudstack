package org.apache.cloudstack.storage.helper;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.cloudstack.acl.Role;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.acl.RoleVO;
import org.apache.cloudstack.acl.dao.RoleDao;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.BaseAsyncCmd;
import org.apache.cloudstack.api.BaseAsyncCreateCmd;
import org.apache.cloudstack.api.command.admin.vm.ScaleVMCmdByAdmin;
import org.apache.cloudstack.api.command.admin.volume.ResizeVolumeCmdByAdmin;
import org.apache.cloudstack.api.command.user.vm.ScaleVMCmd;
import org.apache.cloudstack.api.command.user.volume.ChangeOfferingForVolumeCmd;
import org.apache.cloudstack.api.command.user.volume.ResizeVolumeCmd;
import org.apache.cloudstack.context.CallContext;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStore;
import org.apache.cloudstack.engine.subsystem.api.storage.DataStoreManager;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeDataFactory;
import org.apache.cloudstack.engine.subsystem.api.storage.VolumeInfo;
import org.apache.cloudstack.framework.jobs.AsyncJob;
import org.apache.cloudstack.resourcedetail.DiskOfferingDetailVO;
import org.apache.cloudstack.resourcedetail.dao.DiskOfferingDetailsDao;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolDetailsDao;
import org.apache.cloudstack.storage.datastore.db.StoragePoolVO;
import org.apache.cloudstack.storage.datastore.util.StorPoolUtil;
import org.apache.cloudstack.storage.datastore.util.StorPoolUtil.SpApiResponse;
import org.apache.cloudstack.storage.datastore.util.StorPoolUtil.SpConnectionDesc;
import org.apache.log4j.Logger;

import com.cloud.api.ApiGsonHelper;
import com.cloud.api.ApiServer;
import com.cloud.api.dispatch.DispatchChainFactory;
import com.cloud.api.dispatch.DispatchTask;
import com.cloud.exception.PermissionDeniedException;
import com.cloud.hypervisor.kvm.storage.StorPoolStorageAdaptor;
import com.cloud.service.ServiceOfferingDetailsVO;
import com.cloud.service.dao.ServiceOfferingDetailsDao;
import com.cloud.storage.VolumeVO;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.user.Account;
import com.cloud.user.AccountManager;
import com.cloud.utils.component.ComponentContext;
import com.cloud.utils.component.PluggableService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class StorPoolReplaceCommandsHelper implements PluggableService {
    private static final Logger log = Logger.getLogger(StorPoolReplaceCommandsHelper.class);

    private static StorPoolReplaceCommandsUtil replaceCommandsUtil;
    private ApiServer apiServer;

    public static StorPoolReplaceCommandsUtil getStorPoolReplaceCommandsUtil() {
        if (replaceCommandsUtil == null) {
            replaceCommandsUtil = ComponentContext.inject(StorPoolReplaceCommandsUtil.class);
        }
        return replaceCommandsUtil;
    }

    public void init() {
        this.apiServer = ComponentContext.getComponent(ApiServer.class);
        try {
            // addCommandsBeforeRealInit();
            changeAnnotations();
        } catch (Exception e) {
            log.info(e.getMessage());
        }
    }

    public void addCommandsBeforeRealInit() {
        try {
            Class<ApiServer> cls = ApiServer.class;
            Field field = cls.getDeclaredField("s_apiNameCmdClassMap");
            field.setAccessible(true);
            Object value = field.get(cls);
            Map<String, List<Class<?>>> commandsMap = (Map<String, List<Class<?>>>) value;
            List<Class<?>> set = new ArrayList<>(Arrays.asList(

                    StorPoolResizeVolumeCmd.class, StorPoolResizeVolumeCmdByAdmin.class, StorPoolScaleVMCmd.class,
                    StorPoolScaleVMCmdByAdmin.class));
            for (Class<?> clazz : set) {
                final APICommand at = clazz.getAnnotation(APICommand.class);
                String name = at.name();
                List<Class<?>> cmdList = commandsMap.get(name);
                if (cmdList == null) {
                    cmdList = new ArrayList<>();
                    commandsMap.put(name, cmdList);
                }
                cmdList.add(clazz);
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.error(e.getMessage());
        }
    }

    // Removing real CloudStack commands' names with "". This will help to
    // initialize and work with StorPool's commands
    private static void changeAnnotations() {
        List<Annotation> annotations = new ArrayList<>(
                Arrays.asList(ResizeVolumeCmd.class.getAnnotation(APICommand.class),
                        ResizeVolumeCmdByAdmin.class.getAnnotation(APICommand.class),
                        ScaleVMCmd.class.getAnnotation(APICommand.class),
                        ScaleVMCmdByAdmin.class.getAnnotation(APICommand.class),
                        ChangeOfferingForVolumeCmd.class.getAnnotation(APICommand.class)));
        changeAnnotationValue(annotations, "name", "");
    }

    public static void changeAnnotationValue(List<Annotation> annotations, String key, Object newValue) {
        for (Annotation annotation : annotations) {
            Object handler = Proxy.getInvocationHandler(annotation);
            Field field;
            try {
                field = handler.getClass().getDeclaredField("memberValues");
            } catch (NoSuchFieldException | SecurityException e) {
                throw new IllegalStateException(e);
            }
            field.setAccessible(true);
            Map<String, Object> memberValues;
            try {
                memberValues = (Map<String, Object>) field.get(handler);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
            Object oldValue = memberValues.get(key);
            if (oldValue == null || oldValue.getClass() != newValue.getClass()) {
                throw new IllegalArgumentException();
            }
            memberValues.put(key, newValue);
            StorPoolUtil.spLog("CloudStack command old value=%s, replaced with new value=%s", oldValue, newValue);
        }
    }

    @Override
    public List<Class<?>> getCommands() {
        return new ArrayList<>(Arrays.asList(StorPoolResizeVolumeCmd.class, StorPoolResizeVolumeCmdByAdmin.class,
                StorPoolScaleVMCmd.class, StorPoolScaleVMCmdByAdmin.class, StorPoolChangeOfferingForVolumeCmd.class));
    }

    public static class StorPoolReplaceCommandsUtil {
        @Inject
        private DispatchChainFactory dispatchChainFactory;
        @Inject
        private RoleDao roleDao;
        @Inject
        private AccountManager accountManager;
        @Inject
        private VolumeDataFactory volumeDataFactory;
        @Inject
        private DiskOfferingDetailsDao diskOfferingDetailsDao;
        @Inject
        private ServiceOfferingDetailsDao serviceOfferingDetailsDao;
        @Inject
        private DataStoreManager dataStore;
        @Inject
        private PrimaryDataStoreDao storagePool;
        @Inject
        private StoragePoolDetailsDao storagePoolDetailsDao;

        void ensureCmdHasRequiredValues(BaseAsyncCmd targetCmd, BaseAsyncCmd fakeCmd) {
            if (fakeCmd.getFullUrlParams() != null && targetCmd.getFullUrlParams() == null) {
                dispatchChainFactory.getStandardDispatchChain()
                        .dispatch(new DispatchTask(targetCmd, fakeCmd.getFullUrlParams()));
            }
            if (fakeCmd.getHttpMethod() != null && targetCmd.getHttpMethod() == null) {
                targetCmd.setHttpMethod(fakeCmd.getHttpMethod().toString());
            }
            if (fakeCmd.getResponseType() != null) {
                targetCmd.setResponseType(fakeCmd.getResponseType());
            }
            if (fakeCmd.getFullUrlParams() == null && targetCmd.getFullUrlParams() == null) {
                Type mapType = new TypeToken<Map<String, String>>() {
                }.getType();
                AsyncJob job = (AsyncJob) fakeCmd.getJob();
                targetCmd.setJob(job);
                Gson gson = ApiGsonHelper.getBuilder().create();
                Map<String, String> params = gson.fromJson(job.getCmdInfo(), mapType);
                dispatchChainFactory.getStandardDispatchChain().dispatch(new DispatchTask(targetCmd, params));

                if (targetCmd instanceof BaseAsyncCreateCmd) {
                    BaseAsyncCreateCmd create = (BaseAsyncCreateCmd) targetCmd;
                    create.setEntityId(Long.parseLong(params.get("id")));
                    create.setEntityUuid(params.get("uuid"));
                }
            }
        }

        public boolean hasRights(String value) {
            if (value != null) {
                Account caller = getCurrentAccount();
                if (caller == null || caller.getRoleId() == null) {
                    throw new PermissionDeniedException("Restricted API called by an invalid user account");
                }
                Role callerRole = findRole(caller.getRoleId());
                if (callerRole == null || callerRole.getRoleType() != RoleType.Admin) {
                    throw new PermissionDeniedException(
                            "Restricted API called by an user account of non-Admin role type");
                }
            }
            return true;
        }

        private Role findRole(Long id) {
            if (id == null || id < 1L) {
                log.trace(String.format("Role ID is invalid [%s]", id));
                return null;
            }
            RoleVO role = roleDao.findById(id);
            if (role == null) {
                log.trace(String.format("Role not found [id=%s]", id));
                return null;
            }
            Account account = getCurrentAccount();
            if (!accountManager.isRootAdmin(account.getId()) && RoleType.Admin == role.getRoleType()) {
                log.debug(
                        String.format("Role [id=%s, name=%s] is of 'Admin' type and is only visible to 'Root admins'.",
                                id, role.getName()));
                return null;
            }
            return role;
        }

        private Account getCurrentAccount() {
            return CallContext.current().getCallingAccount();
        }

        public static boolean isStorPoolStorage(PrimaryDataStoreDao primaryStorageDao, VolumeDao volumeDao,
                long volumeId) {
            VolumeVO volume = volumeDao.findById(volumeId);
            if (volume == null || volume.getPoolId() == null) {
                return false;
            }
            StoragePoolVO pool = primaryStorageDao.findById(volume.getPoolId());
            if (pool != null && pool.getStorageProviderName().equals(StorPoolUtil.SP_PROVIDER_NAME)) {
                return true;
            }
            return false;
        }

        public void updateTierTagOrTemplate(long volumeId, long diskOfferingId) {
            VolumeInfo volumeObjectTO = volumeDataFactory.getVolume(volumeId);

            String tier = null;
            String template = null;
            DiskOfferingDetailVO diskOfferingDetail = diskOfferingDetailsDao.findDetail(diskOfferingId,
                    StorPoolUtil.SP_TIER);
            if (diskOfferingDetail == null) {
                StorPoolUtil.spLog("Could not find tier for the storage with disk offering id [%s]. Trying with the SP_TEMPLATE", diskOfferingId);
                diskOfferingDetail = diskOfferingDetailsDao.findDetail(diskOfferingId,
                        StorPoolUtil.SP_TEMPLATE);
                if (diskOfferingDetail == null) {
                    ServiceOfferingDetailsVO serviceOfferingDetail = serviceOfferingDetailsDao.findDetail(diskOfferingId,
                            StorPoolUtil.SP_TEMPLATE);
                    if (serviceOfferingDetail == null) {
                        return;
                    }
                    template = serviceOfferingDetail.getValue();
                } else {
                    template = diskOfferingDetail.getValue();
                }
            } else {
                tier = diskOfferingDetail.getValue();
            }
            DataStore store = dataStore.getPrimaryDataStore(volumeObjectTO.getDataStore().getUuid());
            if (tier != null || template != null) {
                updateTierTagOrTemplate(diskOfferingId, volumeObjectTO, tier, template, store);
            }
        }

        private void updateTierTagOrTemplate(long diskOfferingId, VolumeInfo volumeObjectTO, String tier, String template,
                DataStore store) {
            try {
                SpConnectionDesc conn = StorPoolUtil.getSpConnection(store.getUuid(), store.getId(),
                        storagePoolDetailsDao, storagePool);
                String name = StorPoolStorageAdaptor.getVolumeNameFromPath(volumeObjectTO.getPath(), true);
                SpApiResponse resp = StorPoolUtil.volumeUpadateTierTagsOrTemplate(name, tier, template, conn);
                if (resp.getError() != null) {
                    StorPoolUtil.spLog(
                            "Could not update volume [%s] with the new QOS tag [%s] or template [%s] from the disk offering [%s]", name,
                            tier, template, diskOfferingId);
                }
            } catch (Exception e) {
                StorPoolUtil.spLog(
                        "Could not update volume [%s] with the new QOS tag [%s] or template [%s] from the disk offering [%s]", volumeObjectTO,
                        tier, template, diskOfferingId);
            }
        }
    }
}
