package org.apache.cloudstack.storage.helper;

import javax.inject.Inject;

import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseAsyncCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.command.user.volume.ChangeOfferingForVolumeCmd;
import org.apache.cloudstack.api.response.DiskOfferingResponse;
import org.apache.cloudstack.api.response.VolumeResponse;
import org.apache.cloudstack.storage.datastore.db.PrimaryDataStoreDao;
import org.apache.cloudstack.storage.helper.StorPoolReplaceCommandsHelper.StorPoolReplaceCommandsUtil;
import org.apache.log4j.Logger;

import com.cloud.exception.ConcurrentOperationException;
import com.cloud.exception.InsufficientCapacityException;
import com.cloud.exception.NetworkRuleConflictException;
import com.cloud.exception.ResourceAllocationException;
import com.cloud.exception.ResourceUnavailableException;
import com.cloud.storage.dao.VolumeDao;
import com.cloud.utils.component.ComponentContext;

@APICommand(name = "changeOfferingForVolume",
description = "Change disk offering of the volume and also an option to auto migrate if required to apply the new disk offering",
responseObject = VolumeResponse.class,
requestHasSensitiveInfo = false,
responseHasSensitiveInfo = false,
authorized = { RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User},
since = "4.17")
public class StorPoolChangeOfferingForVolumeCmd extends BaseAsyncCmd {
    public static final String APINAME = "changeofferingforvolumeresponse";

    public static final Logger s_logger = Logger.getLogger(StorPoolChangeOfferingForVolumeCmd.class.getName());

    private ChangeOfferingForVolumeCmd changeOfferingForVolumeCmd;
    private StorPoolReplaceCommandsHelper.StorPoolReplaceCommandsUtil replaceCommands = StorPoolReplaceCommandsHelper
            .getStorPoolReplaceCommandsUtil();

    @Inject
    private PrimaryDataStoreDao primaryStorageDao;
    @Inject
    private VolumeDao volumeDao;

    /////////////////////////////////////////////////////
    //////////////// API parameters /////////////////////
    /////////////////////////////////////////////////////

    @Parameter(name = ApiConstants.ID, entityType = VolumeResponse.class, required = true, type = CommandType.UUID, description = "the ID of the volume")
    private Long id;

    @Parameter(name = ApiConstants.DISK_OFFERING_ID,
            entityType = DiskOfferingResponse.class,
            type = CommandType.UUID,
            required = true,
            description = "new disk offering id")
    private Long newDiskOfferingId;

    @Parameter(name = ApiConstants.SIZE, type = CommandType.LONG, required = false, description = "New volume size in GB for the custom disk offering")
    private Long size;

    @Parameter(name = ApiConstants.MIN_IOPS, type = CommandType.LONG, required = false, description = "New minimum number of IOPS for the custom disk offering")
    private Long minIops;

    @Parameter(name = ApiConstants.MAX_IOPS, type = CommandType.LONG, required = false, description = "New maximum number of IOPS for the custom disk offering")
    private Long maxIops;

    @Parameter(name = ApiConstants.AUTO_MIGRATE, type = CommandType.BOOLEAN, required = false, description = "Flag for automatic migration of the volume " +
            "with new disk offering whenever migration is required to apply the offering")
    private Boolean autoMigrate;

    @Parameter(name = ApiConstants.SHRINK_OK, type = CommandType.BOOLEAN, required = false, description = "Verify OK to Shrink")
    private Boolean shrinkOk;

    @Override
    public String getEventType() {
        return changeOfferingForVolumeCmd.getEventType();
    }

    @Override
    public String getEventDescription() {
        replaceCommands.ensureCmdHasRequiredValues(this.changeOfferingForVolumeCmd, this);
        return changeOfferingForVolumeCmd.getEventDescription();
    }

    public StorPoolChangeOfferingForVolumeCmd() {
        super();
        try {
            this.changeOfferingForVolumeCmd = ChangeOfferingForVolumeCmd.class.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            s_logger.error(e.getMessage());
        }
        this.changeOfferingForVolumeCmd = ComponentContext.inject(this.changeOfferingForVolumeCmd);
    }

    @Override
    public void execute() throws ResourceUnavailableException, InsufficientCapacityException, ServerApiException, ConcurrentOperationException, ResourceAllocationException, NetworkRuleConflictException {
        replaceCommands.ensureCmdHasRequiredValues(this.changeOfferingForVolumeCmd, this);
        this.changeOfferingForVolumeCmd.execute();
        if (StorPoolReplaceCommandsUtil.isStorPoolStorage(primaryStorageDao, volumeDao, id) && newDiskOfferingId != null) {
            replaceCommands.updateTierTagOrTemplate(id, newDiskOfferingId);
        }
        this.setResponseObject(this.changeOfferingForVolumeCmd.getResponseObject());
    }

    @Override
    public String getCommandName() {
        return APINAME;
    }

    @Override
    public long getEntityOwnerId() {
        replaceCommands.ensureCmdHasRequiredValues(this.changeOfferingForVolumeCmd, this);

        return changeOfferingForVolumeCmd.getEntityOwnerId();
    }
}
