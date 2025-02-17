package workflow;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

/**
 * The {@link WorkflowPasser} class is responsible for parsing a DAX (Directed Acyclic Graph in XML) {@link workflow} file
 * and extracting tasks (jobs) and dependencies from it.
 *
 * @author Mohsen Gholami | iMohsen02
 * @see Workflow
 * @see WorkflowPasser
 * @see scheduller.Scheduler
 * @see Broker.MyBroker
 * @see org.cloudbus.cloudsim.Cloudlet
 * @see MyDatacenter.MyDatacenter
 * @see Task
 * @since 2/7/2025
 */
public class WorkflowPasser {

  /**
   * Parses a DAX workflow XML file and extracts jobs and dependencies.
   *
   * @param workflowFilePath the path to the DAX workflow XML file
   * @return a {@link Workflow} object containing tasks and dependencies
   * @throws IOException   if an I/O error occurs while reading the file
   * @throws JDOMException if an error occurs while parsing the XML
   *
   * @see #extractJob(Element)
   * @see #extractDependency(Element)
   */
  public Workflow parse(String workflowFilePath) throws IOException, JDOMException {

    Workflow workflow = new Workflow();
    SAXBuilder builder = new SAXBuilder();
    Document document = builder.build(new File(workflowFilePath));
    Element root = document.getRootElement();

    for (Element child : root.getChildren()) {
      switch (child.getName().toLowerCase()) {
        // Extract jobs in DAX
        case "job" -> workflow.addTask(extractJob(child));
        // Extract dependencies in DAX
        case "child" -> workflow.addDependency(extractDependency(child));
        // Handle unexpected elements
        default -> System.err.println("Unexpected element found: <" + child.getName().toLowerCase() + ">");
      }
    }

    return workflow;
  }

  /**
   * Extracts a dependency from a DAX workflow XML element.
   *
   * @param dependencyNode the XML element representing a dependency
   * @return a {@link Workflow.Dependency} object containing child-parent relationships
   *
   * @see #parse(String)
   * @see Workflow.Dependency
   * @see #extractJob(Element)
   */
  private Workflow.@NotNull Dependency extractDependency(@NotNull Element dependencyNode) {
    Workflow.Dependency dep = new Workflow.Dependency();

    String childId = dependencyNode.getAttributeValue("ref");
    dep.setChild(childId);

    for (Element parent : dependencyNode.getChildren()) dep.addParents(parent.getAttributeValue("ref"));

    return dep;
  }

  /**
   * Extracts a job (task) from a DAX workflow XML element.
   *
   * @param child the XML element representing a job
   * @return a {@code Task} object containing job details
   *
   * @see #extractDependency(Element)
   * @see #parse(String)
   * @see Task
   */
  private @NotNull Task extractJob(@NotNull Element child) {
    String name = child.getAttributeValue("name");
    String daxId = child.getAttributeValue("id");
    String namespace = child.getAttributeValue("namespace");
    double runtime = Double.parseDouble(child.getAttributeValue("runtime"));

    return new Task(daxId, namespace, name, runtime);
  }
}
