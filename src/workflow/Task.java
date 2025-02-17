package workflow;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a task in a workflow that extends the Cloudlet class, that mean it can use as cloudlet in cloudsim.
 * Task is better version of {@link Cloudlet} which can point to its {@link #parents} and {@link #children}.
 * even though able to has more option of a simple cloudlet like {@link #name}, {@link #daxId}, {@link #namespace}, {@link #level}, {@link #runtime}, {@link #daxId}, {@link #isReady} ... and scheduling attributes. <br>
 * appropriate for use in your custom scheduler and broker
 * {@link Workflow}
 *
 * @see Workflow
 * @see WorkflowPasser
 * @see scheduller.Scheduler
 * @see Broker.MyBroker
 * @see org.cloudbus.cloudsim.Cloudlet
 * @see MyDatacenter.MyDatacenter
 *
 * @since 2025
 * @author Mohsen Gholami | iMohsen02
 */
public class Task extends Cloudlet {

  /** Name of the task */
  private final String name;
  /** Unique identifier of the task in DAX format ID00001 */
  private final String daxId;
  /** List of parent tasks (dependencies) */
  private final List<Task> parents;
  /** List of child tasks (dependent tasks) */
  private final List<Task> children;
  /** Namespace to which the task belongs */
  private final String namespace;
  /** Level of the task in the workflow hierarchy */
  private int level;
  /** Execution time of the task in seconds */
  private final double runtime;
  /** Indicates whether the task is ready for execution */
  private boolean isReady;
  /** List of parent tasks that are not yet executed */
  private List<Task> unexecutedParents;
  /** Set of tasks that should execute after this task */
  private final Set<Task> subsequentTasks;
  /** Indicates whether all subsequent tasks discovered yet */
  private boolean isDoneSubsequence;
  /** Indicates task executed or not */
  private boolean execute;

  /**
   * Constructs a new Task with the given parameters.
   *
   * @param daxId     Unique identifier of the task in dax format: ID00001
   * @param namespace Namespace of the task.
   * @param name      Name of the task.
   * @param runtime   Execution time of the task in seconds, A multiple of this is used to calculate {@link Cloudlet#setCloudletLength length}.
   */
  public Task(String daxId, String namespace, String name, double runtime) {
    super(Integer.parseInt(daxId.substring(2)),
      (long) (runtime * 1_000),
      1,
      300,
      300,
      new UtilizationModelFull(),
      new UtilizationModelFull(),
      new UtilizationModelFull());

    this.children = new ArrayList<>();
    this.parents = new ArrayList<>();
    this.subsequentTasks = new HashSet<>();

    this.daxId = daxId;
    this.runtime = runtime;
    this.name = name;
    this.namespace = namespace;

    this.execute = false;
    this.isReady = true;
    this.isDoneSubsequence = true;
    this.level = -1;
  }

  /**
   * Adds a parent task to this task.
   * A parent task must complete execution before this task starts.
   *
   * @param parent The parent task.
   */
  public void addParent(Task parent) {
    this.parents.add(parent);
    // if task has any parent, it means that it has dependency and not ready to execute
    this.isReady = false;
  }

  /**
   * Adds a child task to this task.
   * This task must complete execution before the child task starts.
   *
   * @param child The child task.
   */
  public void addChild(Task child) {
    this.children.add(child);
    // if task has any child, it is not tail tasks in workflow
    this.isDoneSubsequence = false;
  }

  /**
   * Gets the list of child tasks.
   *
   * @return A list of child tasks.
   */
  public List<Task> getChildren() {
    return children;
  }

  /**
   * Gets the list of parent tasks.
   *
   * @return A list of parent tasks.
   */
  public List<Task> getParents() {
    return parents;
  }

  /**
   * Returns a detailed string representation of the task, including dependencies.
   *
   * @return A string describing the task.
   */
  @Override
  public String toString() {
    return "Task {" +
      "\n\ttask = " + this.daxId +
      ", isReady = " + this.isReady +
      ", level = " + this.level +
      ", runtime = " + this.runtime +
      ", length = " + this.getCloudletLength() +
      ", subsequenceTasks = " + getSubsequentTasksLen() +
      ",\n\tparents  = [" + parents.stream().map(c -> c.daxId).collect(Collectors.joining(", ")) + "]" +
      ",\n\tchildren = [" + children.stream().map(c -> c.daxId).collect(Collectors.joining(", ")) + "]" +
      "\n}";
  }

  /**
   * Returns a summarized string representation of the task.
   *
   * @return A summary of the task.
   */
  public String summary() {
    return "Task {" +
      "daxId = " + this.daxId +
      ", isReady = " + this.isReady +
      ", level = " + this.level +
      ", runtime = " + this.runtime +
      ", length = " + this.getCloudletLength() +
      ", parents  = " + this.parents.size() +
      ", children = " + this.children.size() +
      "}";
  }

  /**
   * Adds a task as a subsequent task that should execute after this task.
   *
   * @param task The subsequent task.
   */
  public void addSubsequenceTask(Task task) {
    this.subsequentTasks.add(task);
    this.isDoneSubsequence = true;
  }

  /**
   * Adds multiple subsequent tasks that should execute after this task.
   *
   * @param tasks The set of subsequent tasks.
   */
  public void addSubsequenceTasks(Set<Task> tasks) {
    this.subsequentTasks.addAll(tasks);
    this.isDoneSubsequence = true;
  }

  /**
   * Gets the DAX ID of the task.
   *
   * @return The DAX ID.
   */
  public String getDaxId() {
    return daxId;
  }

  /**
   * Gets the name of the task.
   *
   * @return The task name.
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the execution runtime of the task.
   *
   * @return The runtime in seconds.
   */
  public double getRuntime() {
    return runtime;
  }

  /**
   * Gets the namespace of the task.
   *
   * @return The namespace.
   */
  public String getNamespace() {
    return namespace;
  }

  /**
   * Gets the level of the task in the workflow hierarchy.
   *
   * @return The level.
   */
  public int getLevel() {
    return level;
  }

  /**
   * Checks if the task is ready for execution.
   *
   * @return True if the task is ready, otherwise false.
   */
  public boolean isReady() {
    return this.isReady;
  }

  /**
   * Sets the level of the task in the workflow hierarchy.
   * Ensures the level is not set to a lower value.
   *
   * @param level The level to set.
   */
  public void setLevel(int level) {
    if (level < this.level) return;
    this.level = level;
  }

  /**
   * Checks if all subsequent tasks have been completed.
   *
   * @return True if all subsequent tasks are done, otherwise false.
   */
  public boolean isDoneSubSequence() {
    return this.isDoneSubsequence;
  }

  /**
   * Gets the set of subsequent tasks.
   *
   * @return A set of subsequent tasks.
   */
  public Set<Task> getSubsequentTasks() {
    return subsequentTasks;
  }

  /**
   * Gets the number of subsequent tasks.
   *
   * @return The count of subsequent tasks.
   */
  public int getSubsequentTasksLen() {
    return subsequentTasks.size();
  }

  /**
   * Notifies the task that it has been executed, updating its children.
   * If a child has no remaining unexecuted parents, it becomes ready for execution.
   *
   * @return A list of child tasks that are now ready for execution.
   */
  public List<Task> notifyExecute() {
    // TODO Important notify execute
    List<Task> readyChildren = new ArrayList<>();

    this.execute = true; // task executed
    this.isReady = false; // task executed so it is no longer ready
    for (Task child : this.children) {
      // second solution =>
//      if (child.unexecutedParents == null) { // if it is null none of its parent executed yet
//        child.unexecutedParents = new ArrayList<>(child.parents); // add all its parent to unexecuted parents list
//      }
//      child.unexecutedParents.remove(this); // this task executed already so remove this parent
//
//      if (child.unexecutedParents.isEmpty()) { // if there is no unexecuted parent in list, so it means all its dependencies executed
//        readyChildren.add(child); // add it to ready children to execute on next cycle
//        child.isReady = true; // so it is ready to execute
//      }

      if (child.parents.stream().filter(t -> !t.execute).toList().isEmpty()) {
        readyChildren.add(child);
        child.isReady = true;
      }
    }
    return readyChildren; // return all ready children
  }
}
