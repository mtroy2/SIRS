package edu.nd.sirs.webapp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.jasper.servlet.JspServlet;
import org.apache.tomcat.InstanceManager;
import org.apache.tomcat.SimpleInstanceManager;
import org.eclipse.jetty.annotations.ServletContainerInitializersStarter;
import org.eclipse.jetty.apache.jsp.JettyJasperInitializer;
import org.eclipse.jetty.plus.annotation.ContainerInitializer;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * Simple Jetty Server to run the SIRS Web service
 * 
 * @author tweninge
 *
 */
public class JettyHTTPServer {
	// Resource path pointing to where the WEBROOT is
	private static final String WEBROOT_INDEX = "../../../../webroot/";

	public static void main(String[] args) throws Exception {
		int port = 8080;

		JettyHTTPServer main = new JettyHTTPServer(port);
		main.start();
		main.waitForInterrupt();
	}

	private int port;
	private Server server;
	private URI serverURI;

	public JettyHTTPServer(int port) {
		this.port = port;
	}

	public URI getServerURI() {
		return serverURI;
	}

	public void start() throws Exception {
		server = new Server();
		ServerConnector connector = new ServerConnector(server);
		connector.setPort(port);
		server.addConnector(connector);

		URL indexUri = this.getClass().getResource(".");
		if (indexUri == null) {
			throw new FileNotFoundException("Unable to find resource "
					+ WEBROOT_INDEX);
		}

		// Points to wherever /webroot/ (the resource) is
		URI baseUri = indexUri.toURI();

		// Establish Scratch directory for the servlet context (used by JSP
		// compilation)
		File tempDir = new File(System.getProperty("java.io.tmpdir"));
		File scratchDir = new File(tempDir.toString(), "embedded-jetty-jsp");

		if (!scratchDir.exists()) {
			if (!scratchDir.mkdirs()) {
				throw new IOException("Unable to create scratch directory: "
						+ scratchDir);
			}
		}

		// Set JSP to use Standard JavaC always
		System.setProperty("org.apache.jasper.compiler.disablejsr199", "false");

		// Setup the basic application "context" for this application at "/"
		// This is also known as the handler tree (in jetty speak)
		WebAppContext context = new WebAppContext();
		context.setContextPath("/");
		context.setAttribute("javax.servlet.context.tempdir", scratchDir);
		context.setResourceBase(baseUri.toASCIIString());
		context.setAttribute(InstanceManager.class.getName(),
				new SimpleInstanceManager());
		server.setHandler(context);

		// Ensure the jsp engine is initialized correctly
		JettyJasperInitializer sci = new JettyJasperInitializer();
		ServletContainerInitializersStarter sciStarter = new ServletContainerInitializersStarter(
				context);
		ContainerInitializer initializer = new ContainerInitializer(sci, null);
		List<ContainerInitializer> initializers = new ArrayList<ContainerInitializer>();
		initializers.add(initializer);

		context.setAttribute("org.eclipse.jetty.containerInitializers",
				initializers);
		context.addBean(sciStarter, true);

		// Set Classloader of Context to be sane (needed for JSTL)
		// JSP requires a non-System classloader, this simply wraps the
		// embedded System classloader in a way that makes it suitable
		// for JSP to use
		ClassLoader jspClassLoader = new URLClassLoader(new URL[0], this
				.getClass().getClassLoader());
		context.setClassLoader(jspClassLoader);

		// Add JSP Servlet (must be named "jsp")
		ServletHolder holderJsp = new ServletHolder("jsp", JspServlet.class);
		holderJsp.setInitOrder(0);
		holderJsp.setInitParameter("logVerbosityLevel", "DEBUG");
		holderJsp.setInitParameter("fork", "false");
		holderJsp.setInitParameter("xpoweredBy", "false");
		holderJsp.setInitParameter("compilerTargetVM", "1.7");
		holderJsp.setInitParameter("compilerSourceVM", "1.7");
		holderJsp.setInitParameter("keepgenerated", "true");
		context.addServlet(holderJsp, "*.jsp");
		// context.addServlet(holderJsp,"*.jspf");
		// context.addServlet(holderJsp,"*.jspx");

		// Add Example of mapping jsp to path spec
		ServletHolder holderAltMapping = new ServletHolder("foo.jsp",
				JspServlet.class);
		holderAltMapping.setForcedPath("/test/foo/foo.jsp");
		context.addServlet(holderAltMapping, "/test/foo/");

		// Add Default Servlet (must be named "default")
		ServletHolder holderDefault = new ServletHolder("default",
				DefaultServlet.class);
		// LOG.info("Base URI: " + baseUri);
		holderDefault.setInitParameter("resourceBase", baseUri.toASCIIString());
		holderDefault.setInitParameter("dirAllowed", "true");
		context.addServlet(holderDefault, "/");

		// Start Server
		server.start();

		// Show server state
		// if (LOG.isLoggable(Level.FINE)) {
		// LOG.fine(server.dump());
		// }

		// Establish the Server URI
		String scheme = "http";
		for (ConnectionFactory connectFactory : connector
				.getConnectionFactories()) {
			if (connectFactory.getProtocol().equals("SSL-http")) {
				scheme = "https";
			}
		}
		String host = connector.getHost();
		if (host == null) {
			host = "localhost";
		}
		int port = connector.getLocalPort();
		serverURI = new URI(String.format("%s://%s:%d/", scheme, host, port));
		// LOG.info("Server URI: " + serverURI);
	}

	public void stop() throws Exception {
		server.stop();
	}

	/**
	 * Cause server to keep running until it receives a Interrupt.
	 * <p>
	 * Interrupt Signal, or SIGINT (Unix Signal), is typically seen as a result
	 * of a kill -TERM {pid} or Ctrl+C
	 */
	public void waitForInterrupt() throws InterruptedException {
		server.join();
	}

}
