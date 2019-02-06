package com.parallelm.mlcomp;

import org.apache.log4j.PatternLayout;
import py4j.GatewayServer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;


import java.lang.reflect.Constructor;
import java.util.Map;

class EntryPointConfig {
    public String className;
    public int portNumber;

    @Override
    public String toString() {
        String s = "";
        s += "className: " + className + " portNumber: " + portNumber;
        return s;
    }
}


public class ComponentEntryPoint {
    private MCenterBaseComponent component;

    public ComponentEntryPoint(String className) throws Exception {
        ConsoleAppender console = new ConsoleAppender(); //create appender
        //configure the appender
        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.DEBUG);
        console.activateOptions();
        //add appender to any Logger (here is root)
        Logger.getRootLogger().addAppender(console);
        Logger.getRootLogger().setLevel(Level.INFO);
        System.out.println("Done log4j stuff");
        Class<?> clazz = Class.forName(className);
        Constructor<?> ctor = clazz.getConstructor();
        component = (MCenterBaseComponent) ctor.newInstance();
    }

    public MCenterBaseComponent getComponent() {
        return component;
    }

    public void setMLOps(MLOps mlops) {
        component.setMLOps(mlops);
    }

    private static EntryPointConfig parseArgs(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newFor("ComponentEntryPoint").build()
                                               .defaultHelp(true)
                                               .description("Calculate checksum of given files.");
        parser.addArgument("--class-name")
              .type(String.class)
              .help("Class name to run py4j gateway on");
        parser.addArgument("--port")
              .type(Integer.class)
              .help("Port number to use for py4j gateway");

        Namespace options = null;
        try {
            options = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        Map<String, Object> optionsMap = options.getAttrs();
        EntryPointConfig config = new EntryPointConfig();
        config.className = (String) optionsMap.getOrDefault("class_name", "");
        if (config.className.isEmpty()) {
            throw new Exception("class-name argument must be provided");
        }
        config.portNumber = (int) optionsMap.getOrDefault("port", 0);
        if (config.portNumber == 0) {
            throw new Exception("port must be provided and must be > 0");
        }

        return config;
    }

    public static void main(String[] args) throws Exception {
        try {
            EntryPointConfig config = parseArgs(args);
            ComponentEntryPoint entryPoint = new ComponentEntryPoint(config.className);
            // TODO: use a specific port, note multiple such gateways might be running.
            GatewayServer gatewayServer = new GatewayServer(entryPoint, config.portNumber);
            System.out.println("Java gateway starting, port: " + gatewayServer.getPort());
            gatewayServer.start();

            MLOps mlops = (MLOps) gatewayServer.getPythonServerEntryPoint(new Class[] {MLOps.class});
            entryPoint.setMLOps(mlops);
        } catch (Exception e) {
            System.out.println("Got exception in ComponentEntryPoint: " + e.getMessage());
            throw e;
        }
    }
}
