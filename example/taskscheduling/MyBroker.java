package taskscheduling;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;


public class MyBroker extends DatacenterBroker {


	private int datacenterId = 2;

	private List<Cloudlet> cloudletList;

	public MyBroker(String name) throws Exception {
		super(name);
	}

	@Override
	public void startEntity() {
		cloudletList = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			int id = i;
			long length = 400000;
			long fileSize = 300;
			long outputSize = 300;
			int pesNumber = 1;
			UtilizationModel utilizationModel = new UtilizationModelFull();
			Cloudlet cloudlet = new Cloudlet(id, length, pesNumber, fileSize, outputSize, utilizationModel,
					utilizationModel, utilizationModel);
			cloudlet.setUserId(getId());
			cloudletList.add(cloudlet);
		}

		for (int i = 0; i < 2; i++) {
			int newVmId = i;
			int mips = 1000;
			long size = 10000;
			int ram = 512;
			long bw = 1000;
			int pesNumber = 1; 
			String vmm = "Xen"; 
			Vm vm = new Vm(newVmId, getId(), mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
			
			sendNow(datacenterId, CloudSimTags.VM_CREATE_ACK, vm);

		}
	}

	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {

		// VM Creation answer
		case CloudSimTags.VM_CREATE_ACK:
			processVmCreate(ev);
			break;
		// A finished cloudlet returned
		case CloudSimTags.CLOUDLET_RETURN:
			processCloudletReturn(ev);
			break;
		// if the simulation finishes
		case CloudSimTags.END_OF_SIMULATION:
			shutdownEntity();
			break;
		// other unknown tags are processed by this method
		default:
			processOtherEvent(ev);
			break;
		}
	}

	@Override
	protected void processCloudletReturn(SimEvent ev) {
		// TODO Auto-generated method stub
		Cloudlet cloudlet = (Cloudlet) ev.getData();
		System.out.println(CloudSim.clock()+ ": "+ getName()+
				": Cloudlet "+ cloudlet.getCloudletId()+ " is received");
		
		if(cloudletList.size()!=0) {
			Cloudlet nextCloudlet = cloudletList.remove(0);
			nextCloudlet.setVmId(cloudlet.getVmId());
			sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, nextCloudlet);
		}

	}

	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			System.out.println(CloudSim.clock()+ ": "+ getName()+ ": VM #"+ vmId+ 
					" has been created in Datacenter #"+
					datacenterId);
			if(cloudletList.size()!=0) {
				Cloudlet nextCloudlet = cloudletList.remove(0);
				nextCloudlet.setVmId(vmId);
				sendNow(datacenterId, CloudSimTags.CLOUDLET_SUBMIT, nextCloudlet);
			}
		} else {
			System.out.println(CloudSim.clock()+ ": "+ getName()+ ": Creation of VM #"+ vmId+
					" failed in Datacenter #"+ datacenterId);
		}
	}

	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub
		super.shutdownEntity();
	}

}
