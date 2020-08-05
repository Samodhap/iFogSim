package org.fog.entities.microservicesBased;

import org.apache.commons.math3.util.Pair;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.application.AppEdge;
import org.fog.application.AppModule;
import org.fog.application.Application;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.Tuple;
import org.fog.placement.microservicesBased.MicroservicePlacementLogic;
import org.fog.utils.*;

import java.util.*;

/**
 * Created by Samodha Pallewatta
 */
public class ClusteredFogDevice extends FogDevice {

    // tuple and destination cluster device ID
    protected Queue<Pair<Tuple, Integer>> clusterTupleQueue;
    protected Map<Integer, Double> clusterNodetoLatencyMap;
    protected List<Integer> clusterNodeIds;

    /**
     * Flag denoting whether the link connecting to cluster from this FogDevice is busy
     */
    protected boolean isClusterLinkBusy;
    protected double clusterLinkBandwidth;
    protected boolean isInCluster = false;

    /**
     * Device type (1.client device 2.FCN 3.FON 4.Cloud)
     * in this work client device only holds the clientModule of the app and does not participate in processing and placement of microservices ( microservices can be shared among users,
     * thus for security resons client devices are not used for that)
     */
    protected String deviceType = null;
    public static final String CLIENT = "client";
    public static final String FCN = "fcn"; // fog computation node
    public static final String FON = "fon"; // fog orchestration node
    public static final String CLOUD = "cloud"; // cloud datacenter


    /**
     * closest FON id. If this device is a FON its own id is assigned
     */
    protected int fonID = -1;

    /**
     * used to forward tuples towards the destination device
     * map of <destinationID,nextDeviceID> based on shortest path.
     */
    protected Map<Integer, Integer> routingTable = new HashMap<>();


    protected ControllerComponent controllerComponent;

    protected List<PlacementRequest> placementRequests = new ArrayList<>();

    protected Map<Application, String> clientModulesToBeDeployed = new HashMap<>();

    public ClusteredFogDevice(String name, FogDeviceCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList, double schedulingInterval, double uplinkBandwidth, double downlinkBandwidth, double clusterLinkBandwidth, double uplinkLatency, double ratePerMips, String deviceType) throws Exception {
        super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval, uplinkBandwidth, downlinkBandwidth, uplinkLatency, ratePerMips);

        setClusterLinkBandwidth(clusterLinkBandwidth);
        clusterTupleQueue = new LinkedList<>();
        setClusterLinkBusy(false);
        setClusterNodeIds(new ArrayList<Integer>());
        setClusterNodeToLatency(new HashMap<>());

        setDeviceType(deviceType);

    }

    @Override
    protected void registerOtherEntity() {

        // for energy consumption update
        sendNow(getId(), FogEvents.RESOURCE_MGMT);

        //start periodic placement of placement requests
        //todo check if there's an effect with other devices getting registered. Starts with zero. No effect at this point because of sendNow
        if (getDevicType().equals(ClusteredFogDevice.CLOUD) || getDevicType().equals(ClusteredFogDevice.FON))
            sendNow(getId(), FogEvents.PROCESS_PRS);

        // deploy client modules.
        if (getDevicType().equals(ClusteredFogDevice.CLIENT)) {
            for (Application app : clientModulesToBeDeployed.keySet()) {
                sendNow(getId(), FogEvents.ACTIVE_APP_UPDATE, app);
                sendNow(getId(), FogEvents.APP_SUBMIT, app);
                sendNow(getId(), FogEvents.LAUNCH_MODULE, new AppModule(app.getModuleByName(clientModulesToBeDeployed.get(app))));
            }
        }
    }

    @Override
    protected void processOtherEvent(SimEvent ev) {
        switch (ev.getTag()) {
            case FogEvents.TUPLE_ARRIVAL:
                processTupleArrival(ev);
                break;
            case FogEvents.LAUNCH_MODULE:
                processModuleArrival(ev);
                break;
            case FogEvents.RELEASE_OPERATOR:
                processOperatorRelease(ev);
                break;
            case FogEvents.SENSOR_JOINED:
                processSensorJoining(ev);
                break;
            case FogEvents.SEND_PERIODIC_TUPLE:
                sendPeriodicTuple(ev);
                break;
            case FogEvents.APP_SUBMIT:
                processAppSubmit(ev);
                break;
            case FogEvents.UPDATE_NORTH_TUPLE_QUEUE:
                updateNorthTupleQueue();
                break;
            case FogEvents.UPDATE_SOUTH_TUPLE_QUEUE:
                updateSouthTupleQueue();
                break;
            case FogEvents.UPDATE_CLUSTER_TUPLE_QUEUE:
                updateClusterTupleQueue();
                break;
            case FogEvents.ACTIVE_APP_UPDATE:
                updateActiveApplications(ev);
                break;
            case FogEvents.ACTUATOR_JOINED:
                processActuatorJoined(ev);
                break;
            case FogEvents.LAUNCH_MODULE_INSTANCE:
                updateModuleInstanceCount(ev);
                break;
            case FogEvents.RESOURCE_MGMT:
                manageResources(ev);
                break;
            case FogEvents.PROCESS_PRS:
                processPlacementRequests();
                break;
            case FogEvents.UPDATE_SERVICE_DISCOVERY:
                updateServiceDiscovery(ev);
                break;
            default:
                break;
        }
    }

    public void setInCluster(boolean incluster) {
        isInCluster = incluster;
    }

    public boolean isInCluster() {
        return isInCluster;
    }


    private void updateClusterTupleQueue() {
        if (!getClusterTupleQueue().isEmpty()) {
            Pair<Tuple, Integer> pair = getClusterTupleQueue().poll();
            sendThroughFreeClusterLink(pair.getFirst(), pair.getSecond());
        } else {
            setClusterLinkBusy(false);
        }
    }

    private void sendThroughFreeClusterLink(Tuple tuple, Integer clusterNodeID) {
        double networkDelay = tuple.getCloudletFileSize() / getClusterLinkBandwidth();
        setClusterLinkBusy(true);
        double latency = (getClusterNodeToLatencyMap()).get(clusterNodeID);
        send(getId(), networkDelay, FogEvents.UPDATE_CLUSTER_TUPLE_QUEUE);
        send(clusterNodeID, networkDelay + latency, FogEvents.TUPLE_ARRIVAL, tuple);
        NetworkUsageMonitor.sendingTuple(latency, tuple.getCloudletFileSize());
    }

    protected void sendToCluster(Tuple tuple, int clusterNodeID) {
        if (getClusterNodeIds().contains(clusterNodeID)) {
            if (!isClusterLinkBusy) {
                sendThroughFreeClusterLink(tuple, clusterNodeID);
            } else {
                clusterTupleQueue.add(new Pair<Tuple, Integer>(tuple, clusterNodeID));
            }
        }
    }

    public List<Integer> getClusterNodeIds() {
        return clusterNodeIds;
    }

    public double getClusterLinkBandwidth() {
        return clusterLinkBandwidth;
    }

    public void setClusterNodeToLatency(Map<Integer, Double> clusterNodeToLatency) {
        clusterNodetoLatencyMap = clusterNodeToLatency;
    }

    public Map<Integer, Double> getClusterNodeToLatencyMap() {
        return clusterNodetoLatencyMap;
    }

    public void setClusterNodeIds(List<Integer> nodeIds) {
        clusterNodeIds = nodeIds;
    }

    private void setClusterLinkBusy(boolean busy) {
        this.isClusterLinkBusy = busy;
    }

    private void setClusterLinkBandwidth(double clusterLinkBandwidth) {
        this.clusterLinkBandwidth = clusterLinkBandwidth;
    }

    public Queue<Pair<Tuple, Integer>> getClusterTupleQueue() {
        return clusterTupleQueue;
    }

    protected void setDeviceType(String deviceType) {
        if (deviceType.equals(ClusteredFogDevice.CLIENT) || deviceType.equals(ClusteredFogDevice.FCN) ||
                deviceType.equals(ClusteredFogDevice.FON) || deviceType.equals(ClusteredFogDevice.CLOUD))
            this.deviceType = deviceType;
        else
            Logger.error("Incompatible Device Type", "Device type not included in device type enums in ClusteredFogDevice class");
    }

    public String getDevicType() {
        return deviceType;
    }

    public void addRoutingTable(Map<Integer, Integer> routingTable) {
        this.routingTable = routingTable;
    }

    protected void processTupleArrival(SimEvent ev) {


        TupleM tuple = (TupleM) ev.getData();

        Logger.debug(getName(), "Received tuple " + tuple.getCloudletId() + "with tupleType = " + tuple.getTupleType() + "\t| Source : " +
                CloudSim.getEntityName(ev.getSource()) + "|Dest : " + CloudSim.getEntityName(ev.getDestination()));

        if (deviceType.equals(ClusteredFogDevice.CLOUD)) {
            updateCloudTraffic();
        }

        send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);

        if (FogUtils.appIdToGeoCoverageMap.containsKey(tuple.getAppId())) {
        }

        if (tuple.getDirection() == Tuple.ACTUATOR) {
            sendTupleToActuator(tuple);
            return;
        }

        if (getHost().getVmList().size() > 0) {
            final AppModule operator = (AppModule) getHost().getVmList().get(0);
            if (CloudSim.clock() > 0) {
                getHost().getVmScheduler().deallocatePesForVm(operator);
                getHost().getVmScheduler().allocatePesForVm(operator, new ArrayList<Double>() {
                    protected static final long serialVersionUID = 1L;

                    {
                        add((double) getHost().getTotalMips());
                    }
                });
            }
        }

        if (deviceType.equals(ClusteredFogDevice.CLOUD) && tuple.getDestModuleName() == null) {
            sendNow(getControllerId(), FogEvents.TUPLE_FINISHED, null);
        }

        // these are resultant tuples and created periodic tuples
        if (tuple.getDestinationDeviceId() == -1) {
            // ACTUATOR tuples already handled above. Only UP and DOWN left
            if (tuple.getDirection() == Tuple.UP) {
                int destination = controllerComponent.getDestinationDeviceId(tuple.getDestModuleName());
                tuple.setDestinationDeviceId(destination);
                tuple.setSourceDeviceId(getId());
            } else if (tuple.getDirection() == Tuple.DOWN) {
                int destination = tuple.getDeviceForMicroservice(tuple.getDestModuleName());
                tuple.setDestinationDeviceId(destination);
                tuple.setSourceDeviceId(getId());
            }
        }

        if (tuple.getDestinationDeviceId() == getId()) {
            int vmId = -1;
            for (Vm vm : getHost().getVmList()) {
                if (((AppModule) vm).getName().equals(tuple.getDestModuleName()))
                    vmId = vm.getId();
            }
            if (vmId < 0
                    || (tuple.getModuleCopyMap().containsKey(tuple.getDestModuleName()) &&
                    tuple.getModuleCopyMap().get(tuple.getDestModuleName()) != vmId)) {
                return;
            }
            tuple.setVmId(vmId);
            tuple.addToTraversedMicroservices(getId(), tuple.getDestModuleName());

            updateTimingsOnReceipt(tuple);

            executeTuple(ev, tuple.getDestModuleName());
        } else {
            int nextDeviceToSend = routingTable.get(tuple.getDestinationDeviceId());
            if (nextDeviceToSend == parentId)
                sendUp(tuple);
            else if (childrenIds.contains(nextDeviceToSend))
                sendDown(tuple, nextDeviceToSend);
            else if (clusterNodeIds.contains(nextDeviceToSend))
                sendToCluster(tuple, nextDeviceToSend);
            else
                Logger.error("Routing error", "Routing table of " + getName() + "does not contain next device for destination Id" + tuple.getDestinationDeviceId());
        }
    }

    /**
     * Both cloud and FON participates in placement process
     */
    public void initializeController(LoadBalancer loadBalancer, MicroservicePlacementLogic mPlacement, Map<Integer, Map<String, Double>> resourceAvailability, Map<String, Application> applications, List<FogDevice> fogDevices) {
        if (getDevicType() == ClusteredFogDevice.FON || getDevicType() == ClusteredFogDevice.CLOUD) {
            controllerComponent = new ControllerComponent(getId(), loadBalancer, mPlacement, resourceAvailability, applications, fogDevices);
        } else
            Logger.error("Controller init failed", "FON controller initialized for device " + getName() + " of type " + getDevicType());
    }

    /**
     * FCN and Client devices
     */
    public void initializeController(LoadBalancer loadBalancer) {
        if (getDevicType() != ClusteredFogDevice.FON)
            controllerComponent = new ControllerComponent(getId(), loadBalancer);
    }

    public ControllerComponent getControllerComponent() {
        return controllerComponent;
    }

    public List<PlacementRequest> getPlacementRequests() {
        return placementRequests;
    }

    public void setPlacementRequests(List<PlacementRequest> placementRequests) {
        this.placementRequests = placementRequests;
    }

    public void addPlacementRequests(PlacementRequest placementRequest) {
        placementRequests.add(placementRequest);
    }

    protected void processPlacementRequests() {
        // <prID, <moduleName,deviceId>
        Map<Integer, Map<String, Integer>> placement = getControllerComponent().executeApplicationPlacementLogic(placementRequests);
        //todo it assumed that modules are not shared among applications.
        // <deviceid, < app, list of modules to deploy > this is to remove deploying same module more than once on a certain device.
        Map<Integer, Map<Application, List<String>>> perDevice = new HashMap<>();
        if (placement != null) {
            for (int prID : placement.keySet()) {
                //retrieve application
                PlacementRequest placementRequest = null;
                for (PlacementRequest pr : placementRequests) {
                    if (pr.getPlacementRequestId() == prID)
                        placementRequest = pr;
                }
                Application application = controllerComponent.getApplicationPerId(placementRequest.getApplicationId());
                for (String microserviceName : placement.get(prID).keySet()) {
                    int deviceID = placement.get(prID).get(microserviceName);

                    if (perDevice.containsKey(deviceID)) {
                        if (perDevice.get(deviceID).containsKey(application)) {
                            if (!perDevice.get(deviceID).get(application).contains(microserviceName))
                                perDevice.get(deviceID).get(application).add(microserviceName);
                        }
                        else{
                            Map<Application,List<String>> m = new HashMap<>();
                            perDevice.get(deviceID).put(application,Arrays.asList(microserviceName));
                        }
                    }
                    else{
                        Map<Application,List<String>> m = new HashMap<>();
                        m.put(application,Arrays.asList(microserviceName));
                        perDevice.put(deviceID,m);
                    }

                    //service discovery info propagation
                    List<Integer> clientDevices = getClientServiceNodeIds(application, microserviceName, placementRequest.getPlacedMicroservicesMap(), placement.get(prID));
                    for (int clientDevice : clientDevices) {
                        sendNow(clientDevice, FogEvents.UPDATE_SERVICE_DISCOVERY, new Pair<>(microserviceName, deviceID));
                    }
                }
                placementRequests.remove(placementRequest);
            }

            for(int deviceID:perDevice.keySet()){
                for(Application app:perDevice.get(deviceID).keySet()){
                    //ACTIVE_APP_UPDATE
                    sendNow(deviceID, FogEvents.ACTIVE_APP_UPDATE, app);
                    //APP_SUBMIT
                    sendNow(deviceID, FogEvents.APP_SUBMIT, app);
                    for(String microserviceName:perDevice.get(deviceID).get(app)) {
                        //LAUNCH_MODULE
                        sendNow(deviceID, FogEvents.LAUNCH_MODULE, new AppModule(app.getModuleByName(microserviceName)));
                    }
                }
            }
        }
        send(getId(), Config.PLACEMENT_INTERVAL, FogEvents.PROCESS_PRS);
    }

    public boolean setClientModulesToBeDeplyed(Application app, String moduleName) {
        if (!clientModulesToBeDeployed.keySet().contains(app)) {
            clientModulesToBeDeployed.put(app, moduleName);
            // only one client module per application
            return true;
        } else {
            Logger.error("Duplicate client module", "Client module for same app added already");
            return false;
        }
    }

    public List<Integer> getClientServiceNodeIds(Application application, String
            microservice, Map<String, Integer> placed, Map<String, Integer> placementPerPr) {
        List<String> clientServices = getClientServices(application, microservice);
        List<Integer> nodeIDs = new LinkedList<>();
        for (String clientService : clientServices) {
            if (placed.get(clientService) != null)
                nodeIDs.add(placed.get(clientService));
            else
                nodeIDs.add(placementPerPr.get(microservice));
        }

        return nodeIDs;

    }

    public List<String> getClientServices(Application application, String microservice) {
        List<String> clientServices = new LinkedList<>();

        for (AppEdge edge : application.getEdges()) {
            if (edge.getDestination().equals(microservice) && edge.getDirection() == Tuple.UP)
                clientServices.add(edge.getSource());
        }


        return clientServices;
    }

    protected void updateServiceDiscovery(SimEvent ev) {
        Pair<String, Integer> placement = (Pair) ev.getData();
        this.controllerComponent.addServiceDiscoveryInfo(placement.getFirst(), placement.getSecond());
    }

    protected void processModuleArrival(SimEvent ev) {
        // assumed that a new object of AppModule is sent
        //todo what if an existing module is sent again in another placement cycle -> vertical scaling instead of having two vms
        AppModule module = (AppModule) ev.getData();
        String appId = module.getAppId();
        if (!appToModulesMap.containsKey(appId)) {
            appToModulesMap.put(appId, new ArrayList<String>());
        }
        appToModulesMap.get(appId).add(module.getName());
        processVmCreate(ev, false);
        boolean result = getVmAllocationPolicy().allocateHostForVm(module);
        if (result) {
            getVmList().add(module);
            if (module.isBeingInstantiated()) {
                module.setBeingInstantiated(false);
            }
            initializePeriodicTuples(module);
            module.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(module).getVmScheduler()
                    .getAllocatedMipsForVm(module));

            // if client module remove from map
            if (module.getName().equals("client"))
                clientModulesToBeDeployed.remove(applicationMap.get(appId));

            Logger.debug("Module deploy success", "Module " + module.getName() + " placement on " + getName() + " successful. vm id : " + module.getId());
        } else {
            Logger.error("Module deploy error", "Module " + module.getName() + " placement on " + getName() + " failed");
        }
    }

}
