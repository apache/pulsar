package org.apache.zookeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * It is a utility-wrapper on top of zookeeper shell command which intercepts get operations on managed-ledger and parse
 * binary data to string for readability.
 *
 */
public class ZooKeeperManagedLedgerMain extends ZooKeeperMain {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperManagedLedgerMain.class);

    public ZooKeeperManagedLedgerMain(String[] args) throws IOException, InterruptedException {
        super(args);
    }

    public static void main(String args[]) throws KeeperException, IOException, InterruptedException {
        ZooKeeperManagedLedgerMain main = new ZooKeeperManagedLedgerMain(args);
        main.run(args);
    }

    @Override
    protected boolean processZKCmd(MyCommandOptions co) throws KeeperException, IOException, InterruptedException {

        Stat stat = new Stat();
        String[] args = co.getArgArray();
        String cmd = co.getCommand();
        boolean watch = args.length > 2;

        if (cmd.equals("get") && args.length >= 2) {
            String path = args[1];
            boolean isManagedLedger = path.startsWith("/managed-ledger");
            if (isManagedLedger) {
                try {
                    byte data[] = zk.getData(path, watch, stat);
                    if (data != null) {
                        if (path.split("/").length == 7) { // managed-ledger path
                            try {
                                System.out.println(parseManagedLedgerInfo(data));
                                return true;
                            } catch (Exception e) {
                                System.err.println(String.format("Failed to parse managed-ledger %s due to %s", path,
                                        e.getMessage()));
                            }
                        } else if (path.split("/").length == 8) { // managed-cursor path
                            try {
                                System.out.println(parseManagedCursorInfo(data));
                                return true;
                            } catch (Exception e) {
                                System.err.println(String.format("Failed to parse managed-cursor %s due to %s", path,
                                        e.getMessage()));
                            }
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        return super.processZKCmd(co);
    }

    @SuppressWarnings("unchecked")
    protected void run(String args[]) throws KeeperException, IOException, InterruptedException {

        String command = getInlineCommand(args);

        if (command != null || command.isEmpty()) {
            System.out.println("Welcome to ZooKeeper!");

            boolean jlinemissing = false;
            // only use jline if it's in the classpath
            try {
                Class consoleC = Class.forName("jline.ConsoleReader");
                Class completorC = Class.forName("org.apache.zookeeper.JLineZNodeCompletor");

                Object console = consoleC.getConstructor().newInstance();

                Constructor<?> constructor = completorC.getConstructor(ZooKeeper.class);
                constructor.setAccessible(true);
                Object completor = constructor.newInstance(zk);
                Method addCompletor = consoleC.getMethod("addCompletor", Class.forName("jline.Completor"));
                addCompletor.setAccessible(true);
                addCompletor.invoke(console, completor);

                String line;
                Method readLine = consoleC.getMethod("readLine", String.class);
                while ((line = (String) readLine.invoke(console, getPrompt())) != null) {
                    executeLine(line);
                }
            } catch (ClassNotFoundException e) {
                LOG.debug("Unable to start jline", e);
                jlinemissing = true;
            } catch (NoSuchMethodException e) {
                LOG.debug("Unable to start jline", e);
                jlinemissing = true;
            } catch (InvocationTargetException e) {
                LOG.debug("Unable to start jline", e);
                jlinemissing = true;
            } catch (IllegalAccessException e) {
                LOG.debug("Unable to start jline", e);
                jlinemissing = true;
            } catch (InstantiationException e) {
                LOG.debug("Unable to start jline", e);
                jlinemissing = true;
            }

            if (jlinemissing) {
                System.out.println("JLine support is disabled");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

                String line;
                while ((line = br.readLine()) != null) {
                    executeLine(line);
                }
            }
        } else {
            executeLine(command);
        }

    }

    public String getInlineCommand(String[] args) {
        List<String> argList = Arrays.asList(args);
        Iterator<String> it = argList.iterator();
        StringBuilder command = new StringBuilder();

        while (it.hasNext()) {
            String opt = it.next();
            try {
                if (opt.equals("-server") || opt.equals("-timeout") || opt.equals("-r")) {
                    removeArg(it);
                }
            } catch (NoSuchElementException e) {
                System.err.println("Error: no argument found for option " + opt);
                return null;
            }

            if (!opt.startsWith("-")) {
                command.append(opt);
                command.append(" ");
            }
        }
        return command.toString();
    }

    private static void removeArg(Iterator<String> it) {
        it.remove();
        it.next();
        it.remove();
    }

    private static ManagedLedgerInfo parseManagedLedgerInfo(byte[] data) throws Exception {
        return ((org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.Builder) ManagedLedgerInfo
                .newBuilder().mergeFrom(data)).build();
    }

    private static ManagedCursorInfo parseManagedCursorInfo(byte[] data) throws Exception {
        return ((org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo.Builder) ManagedCursorInfo
                .newBuilder().mergeFrom(data)).build();
    }

}
