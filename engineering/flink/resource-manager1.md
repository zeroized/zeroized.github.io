# Resource Manager(1): Slot的分配与回收

2021/02/18

注：源代码为Flink1.11.2版本

本篇将介绍Flink集群给作业分配、回收Slot的过程及其策略。

## 相关概念

Task Slot是Flink运行时架构中的一个重要部分，其物理意义代表了TaskManager JVM进程中的一个线程，逻辑意义代表了作业中一个算子（或多个共享Slot的算子）的一个并行度。

Slot管理是Flink运行时资源管理的一个重要部分，其相关类位于flink-runtime的```org.apache.flink.runtime.resourcemanager.slotmanager```包中。

## SlotManager

> The slot manager is responsible for maintaining a view on all registered task manager slots, their allocation and all pending slot requests. Whenever a new slot is registered or an allocated slot is freed, then it tries to fulfill another pending slot request. Whenever there are not enough slots available the slot manager will notify the resource manager about it via ResourceActions.allocateResource(WorkerResourceSpec).
>
> In order to free resources and avoid resource leaks, idling task managers (task managers whose slots are currently not used) and pending slot requests time out triggering their release and failure, respectively.

```SlotManager```的工作是维护TaskManager中的Slot，包括以下几项：

- 注册TaskManager中的Slot
- 处理Slot分配请求，并分配Slot
- 释放Slot
- 当Slot不足时申请分配资源
- 维护Slot分配请求和闲置TaskManager的超时触发器

<details>
<summary>SlotManager</summary>

```java
// SlotManager.class
public interface SlotManager extends AutoCloseable {
    int getNumberRegisteredSlots();

    int getNumberRegisteredSlotsOf(InstanceID instanceId);

    int getNumberFreeSlots();

    int getNumberFreeSlotsOf(InstanceID instanceId);

    Map<WorkerResourceSpec, Integer> getRequiredResources();

    ResourceProfile getRegisteredResource();

    ResourceProfile getRegisteredResourceOf(InstanceID instanceID);

    ResourceProfile getFreeResource();

    ResourceProfile getFreeResourceOf(InstanceID instanceID);

    int getNumberPendingSlotRequests();

    void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions);

    void suspend();

    boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException;

    boolean unregisterSlotRequest(AllocationID allocationId);

    boolean registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport);

    boolean unregisterTaskManager(InstanceID instanceId, Exception cause);

    boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport);

    void freeSlot(SlotID slotId, AllocationID allocationId);

    void setFailUnfulfillableRequest(boolean failUnfulfillableRequest);

    @VisibleForTesting
    void unregisterTaskManagersAndReleaseResources();
}
```

</details>

为了实现上述的Slot维护功能，```SlotManagerImpl```实现类维护的数据结构相当丰富，根据其功能大致可以分为几类：

- Slot管理：
    - ```HashMap<SlotID, TaskManagerSlot> slots```所有已注册的Slot
    - ```LinkedHashMap<SlotID, TaskManagerSlot> freeSlots```所有空闲的Slot
    - ```SlotMatchingStrategy slotMatchingStrategy```Slot分配时的匹配策略
    - ```boolean waitResultConsumedBeforeRelease```Slot回收前是否等待所有的消息都被下游消费
- Slot分配请求管理：
    - ```HashMap<AllocationID, SlotID> fulfilledSlotRequests```已满足的Slot分配请求
    - ```HashMap<AllocationID, PendingSlotRequest> pendingSlotRequests```等待中的Slot分配请求
    - ```boolean failUnfulfillableRequest = true```无法满足的Slot分配请求是否直接失败
- TaskManager注册管理：
    - ```HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations```所有已注册的TaskManager
    - ```HashMap<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots```正在注册中的Slot
- Slot Manager自身状态维护和配置：
    - ```ScheduledExecutor scheduledExecutor```定时任务调度器
    - ```Time taskManagerRequestTimeout```分配某个TaskManager中的Slot时TaskManager RPC请求超时时间
    - ```Time slotRequestTimeout```Slot分配请求超时时间
    - ```ScheduledFuture<?> slotRequestTimeoutCheck```Slot分配请求超时定时检测任务异步Future
    - ```Time taskManagerTimeout```闲置TaskManager的超时时间
    - ```ScheduledFuture<?> taskManagerTimeoutCheck```闲置TaskManager定时检测任务异步Future
    - ```Executor mainThreadExecutor```主线程执行器
    - ```boolean started```Slot Manager是否启动
    - ```int maxSlotNum```最大Slot数
- 资源管理和Metrics：
    - ```ResourceManagerId resourceManagerId```资源管理器id
    - ```ResourceActions resourceActions```资源分配回调
    - ```WorkerResourceSpec defaultWorkerResourceSpec```默认worker资源（Cpu资源、堆内外内存、网络内存、管理的内存）
    - ```int numSlotsPerWorker```每个worker对应的Slot数量
    - ```ResourceProfile defaultSlotResourceProfile```默认Slot资源属性
    - ```SlotManagerMetricGroup slotManagerMetricGroup```Slot Manager指标组

## Slot分配请求管理

### 注册Slot分配请求

当ResourceManager请求Slot时，会触发```SlotManager#registerSlotRequest```

<details>
<summary>SlotManagerImpl#registerSlotRequest</summary>

```java
// SlotManagerImpl.class第373行
public boolean registerSlotRequest(SlotRequest slotRequest) throws ResourceManagerException {
	checkInit();

	if (checkDuplicateRequest(slotRequest.getAllocationId())) {
		LOG.debug("Ignoring a duplicate slot request with allocation id {}.", slotRequest.getAllocationId());

		return false;
	} else {
		PendingSlotRequest pendingSlotRequest = new PendingSlotRequest(slotRequest);

		pendingSlotRequests.put(slotRequest.getAllocationId(), pendingSlotRequest);

		try {
			internalRequestSlot(pendingSlotRequest);
		} catch (ResourceManagerException e) {
			// requesting the slot failed --> remove pending slot request
			pendingSlotRequests.remove(slotRequest.getAllocationId());

			throw new ResourceManagerException("Could not fulfill slot request " + slotRequest.getAllocationId() + '.', e);
		}

		return true;
	}
}

// SlotManagerImpl.class第1298行
private boolean checkDuplicateRequest(AllocationID allocationId) {
        return pendingSlotRequests.containsKey(allocationId) || fulfilledSlotRequests.containsKey(allocationId);
}

// SlotManagerImpl.class第860行
private void internalRequestSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
    final ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();

    OptionalConsumer.of(findMatchingSlot(resourceProfile))
        .ifPresent(taskManagerSlot -> allocateSlot(taskManagerSlot, pendingSlotRequest))
        .ifNotPresent(() -> fulfillPendingSlotRequestWithPendingTaskManagerSlot(pendingSlotRequest));
}

private void fulfillPendingSlotRequestWithPendingTaskManagerSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
    ResourceProfile resourceProfile = pendingSlotRequest.getResourceProfile();
    Optional<PendingTaskManagerSlot> pendingTaskManagerSlotOptional = findFreeMatchingPendingTaskManagerSlot(resourceProfile);

    if (!pendingTaskManagerSlotOptional.isPresent()) {
        pendingTaskManagerSlotOptional = allocateResource(resourceProfile);
    }

    OptionalConsumer.of(pendingTaskManagerSlotOptional)
        .ifPresent(pendingTaskManagerSlot -> assignPendingTaskManagerSlot(pendingSlotRequest, pendingTaskManagerSlot))
        .ifNotPresent(() -> {
        // request can not be fulfilled by any free slot or pending slot that can be allocated,
        // check whether it can be fulfilled by allocated slots
            if (failUnfulfillableRequest && !isFulfillableByRegisteredOrPendingSlots(pendingSlotRequest.getResourceProfile())) {
                throw new UnfulfillableSlotRequestException(pendingSlotRequest.getAllocationId(), pendingSlotRequest.getResourceProfile());
            }
        });
}

// SlotManagerImpl.class第622行
private Optional<TaskManagerSlot> findMatchingSlot(ResourceProfile requestResourceProfile) {
    final Optional<TaskManagerSlot> optionalMatchingSlot = slotMatchingStrategy.findMatchingSlot(
        requestResourceProfile,
        freeSlots.values(),
        this::getNumberRegisteredSlotsOf);

    optionalMatchingSlot.ifPresent(taskManagerSlot -> {
        // sanity check
        Preconditions.checkState(
        taskManagerSlot.getState() == TaskManagerSlot.State.FREE,
        "TaskManagerSlot %s is not in state FREE but %s.",
        taskManagerSlot.getSlotId(), taskManagerSlot.getState());

        freeSlots.remove(taskManagerSlot.getSlotId());
    });

    return optionalMatchingSlot;
}
```
</details>

在注册Slot分配请求时，首先经过AllocationId去重，检查在等待中的Slot分配请求表和已满足的Slot分配请求表中是否包含相同的AllocationId；然后创建一个等待中的Slot分配请求，并在所有空闲Slot中匹配是否有满足条件的Slot，如果有则直接分配；如果在空闲Slot中找不到满足条件的Slot，则在等待注册中的Slot中匹配是否有满足条件的Slot；如果依然匹配不到，则根据```failUnfulfillableRequest```的配置直接fail该请求或是继续让该请求排队

#### Slot匹配策略

Flink提供了两种Slot匹配策略：默认的```AnyMatchingSlotMatchingStrategy```和```LeastUtilizationSlotMatchingStrategy```。

<details>
<summary>AnyMatchingSlotMatchingStrategy</summary>

```java
// AnyMatchingSlotMatchingStrategy.class
public enum AnyMatchingSlotMatchingStrategy implements SlotMatchingStrategy {
	INSTANCE;

	@Override
	public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
			ResourceProfile requestedProfile,
			Collection<T> freeSlots,
			Function<InstanceID, Integer> numberRegisteredSlotsLookup) {

		return freeSlots.stream().filter(slot -> slot.isMatchingRequirement(requestedProfile)).findAny();
	}
}
```
</details>

在默认模式下，Flink的Slot匹配策略使用```AnyMatchingSlotMatchingStrategy```，直接遍历整个空闲Slot列表，一旦找到匹配的就返回该Slot。由于freeSlots是一个**LinkedHashMap**，因此在第一轮分配Slot时，会按照Slot加入的顺序进行遍历，即TaskManager加入集群的顺序。所以很多时候，Flink集群会将所有的作业全都分配到同一个TaskManager上。

<details>
<summary>LeastUtilizationSlotMatchingStrategy</summary>

```java
// LeastUtilizationSlotMatchingStrategy.class
public enum LeastUtilizationSlotMatchingStrategy implements SlotMatchingStrategy {
	INSTANCE;

	@Override
	public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
			ResourceProfile requestedProfile,
			Collection<T> freeSlots,
			Function<InstanceID, Integer> numberRegisteredSlotsLookup) {
		final Map<InstanceID, Integer> numSlotsPerTaskExecutor = freeSlots.stream()
			.collect(Collectors.groupingBy(
				TaskManagerSlotInformation::getInstanceId,
				Collectors.reducing(0, i -> 1, Integer::sum)));

		return freeSlots.stream()
			.filter(taskManagerSlot -> taskManagerSlot.isMatchingRequirement(requestedProfile))
			.min(Comparator.comparingDouble(taskManagerSlot -> calculateUtilization(taskManagerSlot.getInstanceId(), numberRegisteredSlotsLookup, numSlotsPerTaskExecutor)));
	}

	private static double calculateUtilization(InstanceID instanceId, Function<? super InstanceID, Integer> numberRegisteredSlotsLookup, Map<InstanceID, Integer> numSlotsPerTaskExecutor) {
		final int numberRegisteredSlots = numberRegisteredSlotsLookup.apply(instanceId);

		Preconditions.checkArgument(numberRegisteredSlots > 0, "The TaskExecutor %s has no slots registered.", instanceId);

		final int numberFreeSlots = numSlotsPerTaskExecutor.getOrDefault(instanceId, 0);

		Preconditions.checkArgument(numberRegisteredSlots >= numberFreeSlots, "The TaskExecutor %s has fewer registered slots than free slots.", instanceId);

		return (double) (numberRegisteredSlots - numberFreeSlots) / numberRegisteredSlots;
	}
}
```
</details>

当配置了```cluster.evenly-spread-out-slots: true```配置项后，Slot匹配策略采用```LeastUtilizationSlotMatchingStrategy```。此时，Flink会计算每个TaskManager的使用率分数：$ 已使用的Slot数/总Slot数 $，并分配满足条件且对应TaskManager使用率分数最低的Slot。

### 注销Slot分配请求

当ResourceManager取消Slot请求时，触发```SlotManager@unregisterSlotRequest```方法。

<details>
<summary>SlotManagerImpl#unregisterSlotRequest</summary>

```java
// SlotManagerImpl.class第406行
public boolean unregisterSlotRequest(AllocationID allocationId) {
	checkInit();

	PendingSlotRequest pendingSlotRequest = pendingSlotRequests.remove(allocationId);

	if (null != pendingSlotRequest) {
		LOG.debug("Cancel slot request {}.", allocationId);

		cancelPendingSlotRequest(pendingSlotRequest);

		return true;
	} else {
		LOG.debug("No pending slot request with allocation id {} found. Ignoring unregistration request.", allocationId);

		return false;
	}
}

// SlotManagerImpl.class第1188行
private void cancelPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
    CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

    returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

    if (null != request) {
        request.cancel(false);
    }
}

// SlotManagerImpl.class第1028行
private void returnPendingTaskManagerSlotIfAssigned(PendingSlotRequest pendingSlotRequest) {
    final PendingTaskManagerSlot pendingTaskManagerSlot = pendingSlotRequest.getAssignedPendingTaskManagerSlot();
    if (pendingTaskManagerSlot != null) {
        pendingTaskManagerSlot.unassignPendingSlotRequest();
        pendingSlotRequest.unassignPendingTaskManagerSlot();
    }
}
```
</details>

注销Slot分配请求只能取消等待中的Slot分配请求。注销Slot分配请求后，```SlotManager```会尝试移除Slot分配请求和已经分配给该请求的Slot中的分配信息。

## Slot管理

### 分配Slot

分配Slot既涉及到物理上的Slot分配，也涉及到SlotManager中对Slot的逻辑分配：

<details>
<summary>SlotManagerImpl#allocateSlot</summary>

```java
// SlotManagerImpl.class第958行
private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
	Preconditions.checkState(taskManagerSlot.getState() == TaskManagerSlot.State.FREE);

	TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
	TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

	final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
	final AllocationID allocationId = pendingSlotRequest.getAllocationId();
	final SlotID slotId = taskManagerSlot.getSlotId();
	final InstanceID instanceID = taskManagerSlot.getInstanceId();

	taskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
	pendingSlotRequest.setRequestFuture(completableFuture);

	returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

	TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);

	if (taskManagerRegistration == null) {
		throw new IllegalStateException("Could not find a registered task manager for instance id " +
			instanceID + '.');
	}

	taskManagerRegistration.markUsed();

	// RPC call to the task manager
	CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
		slotId,
		pendingSlotRequest.getJobId(),
		allocationId,
		pendingSlotRequest.getResourceProfile(),
		pendingSlotRequest.getTargetAddress(),
		resourceManagerId,
		taskManagerRequestTimeout);

	requestFuture.whenComplete(
		(Acknowledge acknowledge, Throwable throwable) -> {
			if (acknowledge != null) {
				completableFuture.complete(acknowledge);
			} else {
				completableFuture.completeExceptionally(throwable);
			}
		});

	completableFuture.whenCompleteAsync(
		(Acknowledge acknowledge, Throwable throwable) -> {
			try {
				if (acknowledge != null) {
					updateSlot(slotId, allocationId, pendingSlotRequest.getJobId());
				} else {
					if (throwable instanceof SlotOccupiedException) {
						SlotOccupiedException exception = (SlotOccupiedException) throwable;
						updateSlot(slotId, exception.getAllocationId(), exception.getJobId());
					} else {
						removeSlotRequestFromSlot(slotId, allocationId);
					}

					if (!(throwable instanceof CancellationException)) {
						handleFailedSlotRequest(slotId, allocationId, throwable);
					} else {
						LOG.debug("Slot allocation request {} has been cancelled.", allocationId, throwable);
					}
				}
			} catch (Exception e) {
				LOG.error("Error while completing the slot allocation.", e);
			}
		},
		mainThreadExecutor);
}
```
</details>

首先进行Slot的逻辑分配，在Slot的数据结构中记录Slot分配请求，并在Slot分配请求中记录TaskManager对物理分配结果的响应Future；然后进行Slot的物理分配，Slot Manager通过```TaskExecutorGateway#requestSlot```方法向Task Executor请求一个Slot，根据异步响应的结果可能发生以下几种可能：
- 正确返回了ack，直接更新Slot的状态
- 返回了异常
  1. 返回了待分配的Slot已被占用的异常（```SlotOccupiedException```），更新Slot状态（更新为已占用该Slot的AllocationId和JobId）；如果不是，则从注册的TaskManager中移除该等待分配的Slot请求，并将该待分配Slot状态清除
  2. 如果异常是一个取消异常（```CancellationException```，表示Slot分配异步请求被取消），则取消Slot分配，结束分配流程，此时Slot和Slot分配请求都处于pending状态；如果不是一个取消异常，表明是一个分配失败异常，则通过```SlotManagerImpl#internalRequestSlot```方法立马再请求一次Slot，试图匹配一个新的Slot：
    - 如果匹配成功，则再进行一次Slot分配的流程
    - 如果匹配失败，但在pending Slot表中匹配成功，将匹配到的Slot和Slot分配请求置为pending
    - 如果在pending Slot表中也匹配失败，则从等待中的Slot分配请求表中移除该Slot分配请求，触发资源分配失败动作```ResourceActions#notifyAllocationFailure```通知JobManager

### 释放Slot

<details>
<summary>SlotManager#freeSlot</summary>

```java
// SlotManagerImpl.class第532行
public void freeSlot(SlotID slotId, AllocationID allocationId) {
	checkInit();

	TaskManagerSlot slot = slots.get(slotId);

	if (null != slot) {
		if (slot.getState() == TaskManagerSlot.State.ALLOCATED) {
			if (Objects.equals(allocationId, slot.getAllocationId())) {

				TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

				if (taskManagerRegistration == null) {
					throw new IllegalStateException("Trying to free a slot from a TaskManager " +
						slot.getInstanceId() + " which has not been registered.");
				}

				updateSlotState(slot, taskManagerRegistration, null, null);
			} else {
				LOG.debug("Received request to free slot {} with expected allocation id {}, " +
					"but actual allocation id {} differs. Ignoring the request.", slotId, allocationId, slot.getAllocationId());
			}
		} else {
			LOG.debug("Slot {} has not been allocated.", allocationId);
		}
	} else {
		LOG.debug("Trying to free a slot {} which has not been registered. Ignoring this message.", slotId);
	}
}
```
</details>

释放Slot的过程比较简单，如果待释放的Slot存在且确实是Allocated状态时，触发一次Slot状态更新，清除其AllocationId和JobId状态

### 更新Slot状态

分配Slot和释放Slot都涉及到对Slot状态的更新。Slot状态更新根据新状态是否包含AllocationId分为两种情况，根据Slot当前状态又各自分为三种情况：

<details>
<summary>SlotManagerImpl#updateSlot</summary>

```java
// SlotManagerImpl.class第776行
private boolean updateSlot(SlotID slotId, AllocationID allocationId, JobID jobId) {
	final TaskManagerSlot slot = slots.get(slotId);

	if (slot != null) {
		final TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

		if (taskManagerRegistration != null) {
			updateSlotState(slot, taskManagerRegistration, allocationId, jobId);

			return true;
		} else {
			throw new IllegalStateException("Trying to update a slot from a TaskManager " +
				slot.getInstanceId() + " which has not been registered.");
		}
	} else {
		LOG.debug("Trying to update unknown slot with slot id {}.", slotId);

		return false;
	}
}

private void updateSlotState(
		TaskManagerSlot slot,
		TaskManagerRegistration taskManagerRegistration,
		@Nullable AllocationID allocationId,
		@Nullable JobID jobId) {
	if (null != allocationId) {
		switch (slot.getState()) {
			case PENDING:
				// we have a pending slot request --> check whether we have to reject it
				PendingSlotRequest pendingSlotRequest = slot.getAssignedSlotRequest();

				if (Objects.equals(pendingSlotRequest.getAllocationId(), allocationId)) {
					// we can cancel the slot request because it has been fulfilled
					cancelPendingSlotRequest(pendingSlotRequest);

					// remove the pending slot request, since it has been completed
					pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());

					slot.completeAllocation(allocationId, jobId);
				} else {
					// we first have to free the slot in order to set a new allocationId
					slot.clearPendingSlotRequest();
					// set the allocation id such that the slot won't be considered for the pending slot request
					slot.updateAllocation(allocationId, jobId);

					// remove the pending request if any as it has been assigned
					final PendingSlotRequest actualPendingSlotRequest = pendingSlotRequests.remove(allocationId);

					if (actualPendingSlotRequest != null) {
						cancelPendingSlotRequest(actualPendingSlotRequest);
					}

					// this will try to find a new slot for the request
					rejectPendingSlotRequest(
						pendingSlotRequest,
						new Exception("Task manager reported slot " + slot.getSlotId() + " being already allocated."));
				}

				taskManagerRegistration.occupySlot();
				break;
			case ALLOCATED:
				if (!Objects.equals(allocationId, slot.getAllocationId())) {
					slot.freeSlot();
					slot.updateAllocation(allocationId, jobId);
				}
				break;
			case FREE:
				// the slot is currently free --> it is stored in freeSlots
				freeSlots.remove(slot.getSlotId());
				slot.updateAllocation(allocationId, jobId);
				taskManagerRegistration.occupySlot();
				break;
		}

		fulfilledSlotRequests.put(allocationId, slot.getSlotId());
	} else {
		// no allocation reported
		switch (slot.getState()) {
			case FREE:
				handleFreeSlot(slot);
				break;
			case PENDING:
				// don't do anything because we still have a pending slot request
				break;
			case ALLOCATED:
				AllocationID oldAllocation = slot.getAllocationId();
				slot.freeSlot();
				fulfilledSlotRequests.remove(oldAllocation);
				taskManagerRegistration.freeSlot();

				handleFreeSlot(slot);
				break;
		}
	}
}

// SlotManagerImpl.class第1042行
private void handleFreeSlot(TaskManagerSlot freeSlot) {
    Preconditions.checkState(freeSlot.getState() == TaskManagerSlot.State.FREE);

    PendingSlotRequest pendingSlotRequest = findMatchingRequest(freeSlot.getResourceProfile());

    if (null != pendingSlotRequest) {
        allocateSlot(freeSlot, pendingSlotRequest);
    } else {
        freeSlots.put(freeSlot.getSlotId(), freeSlot);
    }
}
```
</details>

- 新状态包含AllocationId，在完成操作后将AllocationId对应的Slot分配请求添加到已满足的Slot分配请求表中：
  - Slot状态是Pending：如果AllocationId和Pending的AllocationId一致，则认为Slot分配请求实际上已经完成，移除等待中的Slot分配请求，并将Slot状态更新为Allocated；如果不一致，则先将Slot的AllocationId改为新的，然后移除新AllocationId对应的Slot分配请求，最后拒绝原先AllocationId对应的Slot分配请求
  - Slot状态是Allocated：如果AllocationId和原先注册的不一致，更新为新的
  - Slot状态是Free：从freeSlots中移除该Slot，并使用AllocationId标记该Slot
- 新状态不包含AllocationId：
  - Slot状态是Free：寻找所有等待中的Slot分配请求，如果有能匹配的，满足该Slot分配请求
  - Slot状态是Pending：什么都不做
  - Slot状态是Allocated：先将该Slot闲置，然后重复Free状态的操作，将其分配给可能的Slot分配请求

### 注册Slot

<details>
<summary>SlotManagerImpl#registerSlot</summary>

```java
// SlotManagerImpl.class第655行
private void registerSlot(
		SlotID slotId,
		AllocationID allocationId,
		JobID jobId,
		ResourceProfile resourceProfile,
		TaskExecutorConnection taskManagerConnection) {

	if (slots.containsKey(slotId)) {
		// remove the old slot first
		removeSlot(
			slotId,
			new SlotManagerException(
				String.format(
					"Re-registration of slot %s. This indicates that the TaskExecutor has re-connected.",
					slotId)));
	}

	final TaskManagerSlot slot = createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);

	final PendingTaskManagerSlot pendingTaskManagerSlot;

	if (allocationId == null) {
		pendingTaskManagerSlot = findExactlyMatchingPendingTaskManagerSlot(resourceProfile);
	} else {
		pendingTaskManagerSlot = null;
	}

	if (pendingTaskManagerSlot == null) {
		updateSlot(slotId, allocationId, jobId);
	} else {
		pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
		final PendingSlotRequest assignedPendingSlotRequest = pendingTaskManagerSlot.getAssignedPendingSlotRequest();

		if (assignedPendingSlotRequest == null) {
			handleFreeSlot(slot);
		} else {
			assignedPendingSlotRequest.unassignPendingTaskManagerSlot();
			allocateSlot(slot, assignedPendingSlotRequest);
		}
	}
}
```
</details>

如果SlotId在表中已存在，则先触发一次移除Slot。在注册过程中，首先向Slot表中添加一个新的Slot，此时该Slot的状态为空。由于注册Slot可能由TaskManager重连触发，因此注册的Slot可能被正在运行中的Job占用：
- 如果Slot已有AllocationId和JobId，则更新Slot的状态
- 如果Slot无AllocationId和JobId，则试图在现有等待中的Slot找到一个完全一致的Slot，如果找到的Slot已经被分配了Slot分配请求，将找到的Slot对应的分配请求转移到自己，然后触发Slot分配过程

### 移除Slot

<details>
<summary>SlotManagerImpl#removeSlots</summary>

```java
// SlotManagerImpl.class第1060行
private void removeSlots(Iterable<SlotID> slotsToRemove, Exception cause) {
    for (SlotID slotId : slotsToRemove) {
        removeSlot(slotId, cause);
    }
}
        
private void removeSlot(SlotID slotId, Exception cause) {
    TaskManagerSlot slot = slots.remove(slotId);

    if (null != slot) {
        freeSlots.remove(slotId);

        if (slot.getState() == TaskManagerSlot.State.PENDING) {
        // reject the pending slot request --> triggering a new allocation attempt
            rejectPendingSlotRequest(
                slot.getAssignedSlotRequest(),
                cause);
        }

        AllocationID oldAllocationId = slot.getAllocationId();

        if (oldAllocationId != null) {
            fulfilledSlotRequests.remove(oldAllocationId);

            resourceActions.notifyAllocationFailure(
                slot.getJobId(),
                oldAllocationId,
                cause);
        }
    } else {
        LOG.debug("There was no slot registered with slot id {}.", slotId);
    }
}
```
</details>

根据Slot的状态，移除Slot的步骤有所不同：
- Slot状态是Free：直接移除
- Slot状态是Pending：移除并拒绝Slot对应的Slot分配请求
- Slot状态是Allocated：移除并通知ResourceManager分配失败

## TaskManager的注册与注销

### 注册TaskManager

当TaskManager启动时或是尝试重连时（```RegisteredRpcConnection#tryReconnect```）会尝试向ResourceManager注册自己，在注册成功后的回调中触发```SlotManager#registerTaskManager```方法。

<details>
<summary>SlotManager#registerTaskManager</summary>

```java
// SlotManagerImpl.class第433行
public boolean registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
	checkInit();

	LOG.debug("Registering TaskManager {} under {} at the SlotManager.", taskExecutorConnection.getResourceID(), taskExecutorConnection.getInstanceID());

	// we identify task managers by their instance id
	if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
		reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
		return false;
	} else {
		if (isMaxSlotNumExceededAfterRegistration(initialSlotReport)) {
			LOG.info("The total number of slots exceeds the max limitation {}, release the excess resource.", maxSlotNum);
			resourceActions.releaseResource(taskExecutorConnection.getInstanceID(), new FlinkException("The total number of slots exceeds the max limitation."));
			return false;
		}

		// first register the TaskManager
		ArrayList<SlotID> reportedSlots = new ArrayList<>();

		for (SlotStatus slotStatus : initialSlotReport) {
			reportedSlots.add(slotStatus.getSlotID());
		}

		TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
			taskExecutorConnection,
			reportedSlots);

		taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);

		// next register the new slots
		for (SlotStatus slotStatus : initialSlotReport) {
			registerSlot(
				slotStatus.getSlotID(),
				slotStatus.getAllocationID(),
				slotStatus.getJobID(),
				slotStatus.getResourceProfile(),
				taskExecutorConnection);
		}

		return true;
	}

}
```
</details>

注册TaskManager分为两种情况：
- 要注册的TaskManager已经在注册表中了，此时进行TaskManager中所有Slot状态的更新
- 要注册的TaskManager还未注册，此时注册TaskManager并注册TaskManager中所有的Slot

### 注销TaskManager

有4种情况会触发TaskManager的注销：
- TaskManager断开连接时
- 注册时TaskManager已存在，此时先注销旧的TaskManager再注册新的
- 注册时超过了最大Slot数，此时会触发ResourceManager的资源释放，注销TaskManager
- 当ResourceManager失去Leader地位时，触发suspend过程

<details>
<summary>SlotManagerImpl#unregisterTaskManager</summary>

```java
// SlotManagerImpl.class第478行
public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
	checkInit();

	LOG.debug("Unregister TaskManager {} from the SlotManager.", instanceId);

	TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.remove(instanceId);

	if (null != taskManagerRegistration) {
		internalUnregisterTaskManager(taskManagerRegistration, cause);

		return true;
	} else {
		LOG.debug("There is no task manager registered with instance ID {}. Ignoring this message.", instanceId);

		return false;
	}
}

// SlotManagerImpl.class第1292行
private void internalUnregisterTaskManager(TaskManagerRegistration taskManagerRegistration, Exception cause) {
    Preconditions.checkNotNull(taskManagerRegistration);

    removeSlots(taskManagerRegistration.getSlots(), cause);
}

```
</details>

注销TaskManager的操作比较简单，就是移除TaskManager中所有的Slot。