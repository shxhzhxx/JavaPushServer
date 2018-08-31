package shxhzhxx;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;

public class PushServer {
    public static void main(String[] args) {
        PushServer server = new PushServer(3889, 1024);
        server.start();
    }

    private final int MAX_MESSAGE_SIZE;
    private final int PORT;
    private final Map<Integer, Attachment> map = new HashMap<>();

    public PushServer(int port, int maxMessageSize) {
        PORT = port;
        MAX_MESSAGE_SIZE = maxMessageSize;
    }

    public void start() {
        map.clear();
        Selector selector = null;
        ServerSocketChannel server = null;
        ByteBuffer buff = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
        try {
            selector = Selector.open();
            server = ServerSocketChannel.open();
            server.configureBlocking(false);
            server.bind(new InetSocketAddress(PORT));
            server.register(selector, server.validOps());
        } catch (IOException e) {
            log("init failed: " + e.getMessage());
            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException ignored) {
                }
            }
            if (server != null) {
                try {
                    server.close();
                } catch (IOException ignored) {
                }
            }
            return;
        }
        log("init success");
        test();

        for (; ; ) {
            try {
                selector.select();
            } catch (IOException e) {
                log("select exception: " + e.getMessage());
                try {
                    selector.close();
                } catch (IOException ignored) {
                }
                return;
            }
            for (SelectionKey key : selector.selectedKeys()) {
                if (!key.isValid()) {
                    // 这里的key之所以可能invalid是因为，在循环select的key集合时，先处理的key可能close了后处理的key的channel，
                    // 具体场景是先处理的key绑定了后处理key的id，导致后处理的key的channel被close。
                    continue;
                }
                if (key.isAcceptable()) {
                    SocketChannel socketChannel = null;
                    try {
                        socketChannel = server.accept();
                        socketChannel.configureBlocking(false);
                        socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                        socketChannel.register(selector, SelectionKey.OP_READ, new Attachment(socketChannel));
                    } catch (IOException e) {
                        log("accept exception: " + e.getMessage());
                        try {
                            selector.close();
                        } catch (IOException ignored) {
                        }
                        try {
                            server.close();
                        } catch (IOException ignored) {
                        }
                        if (socketChannel != null) {
                            try {
                                socketChannel.close();
                            } catch (IOException ignored) {
                            }
                        }
                        return;
                    }
                } else {
                    Attachment at = (Attachment) key.attachment();
                    if (at.byteBuffer.position() < 4) {
                        at.byteBuffer.limit(4);
                        if (at.read() < 0) {
                            log("broken pipe");
                            //broken pipe
                            continue;
                        }
                        if (at.byteBuffer.position() < 4) {
                            //wait more data
                            continue;
                        }
                    }
                    int len = at.byteBuffer.getInt(0);
                    if (len > MAX_MESSAGE_SIZE || len < 5) {
                        //invalid len
                        log("invalid len: " + len);
                        if (at.id > 0)
                            map.remove(at.id);
                        at.close();
                        continue;
                    }
                    at.byteBuffer.limit(len);
                    if (at.read() < 0) {
                        log("broken pipe");
                        //broken pipe
                        continue;
                    }
                    if (at.byteBuffer.position() < len) {
                        continue;//wait more data
                    }
                    switch (at.byteBuffer.get(4)) {
                        default://unknown cmd
                            log("unknown cmd: " + at.byteBuffer.get(4));
                            if (at.id > 0)
                                map.remove(at.id);
                            at.close();
                            break;
                        case 1://bind
                            if (at.id != 0) {//already bind
                                log("already bind: " + at.id);
                                map.remove(at.id);
                                at.close();
                            } else if (len != 9) {//invalid param
                                log("bind invalid param, len(%d)!=9", len);
                                at.close();
                            } else {
                                int id = at.byteBuffer.getInt(5);
                                if (id == 0) {//invalid param
                                    log("bind invalid param, id=0");
                                    at.close();
                                } else {
                                    at.id = id;
                                    at.byteBuffer.clear();
                                    Attachment prev = map.put(id, at);
                                    if (prev != null) {
                                        prev.close();
                                    }
                                }
                            }
                            break;
                        case 2://single push
                            if (len < 9) {
                                log("single push invalid param, len(%d)<9", len);
                                if (at.id > 0)
                                    map.remove(at.id);
                                at.close();
                            } else {
                                Attachment target = map.get(at.byteBuffer.getInt(5));
                                if (target != null) {
                                    at.byteBuffer.putInt(5, len - 5);
                                    at.byteBuffer.position(5);
                                    if (at.byteBuffer.remaining() != target.write(at.byteBuffer)) {
                                        //broken pipe
                                        log("broken pipe");
                                        target.close();
                                    }
                                }
                                at.byteBuffer.clear();
                            }
                            break;
                        case 3://multi push
                            if (len < 9) {
                                log("multi push invalid param, len(%d)<9", len);
                                if (at.id > 0)
                                    map.remove(at.id);
                                at.close();
                            } else {
                                int num = at.byteBuffer.getInt(5);
                                if (len < 9 + num * 4) {
                                    log("multi push invalid param, len(%d)<9+num(%d)*4", len, num);
                                    if (at.id > 0)
                                        map.remove(at.id);
                                    at.close();
                                } else {
                                    at.byteBuffer.position(5 + 4 * num);
                                    buff.clear();
                                    buff.put(at.byteBuffer);
                                    buff.putInt(0, len - 5 - 4 * num);
                                    buff.limit(len - 5 - 4 * num);
                                    for (int i = 0; i < num; ++i) {
                                        Attachment target = map.get(at.byteBuffer.getInt(9 + 4 * i));
                                        if (target != null) {
                                            buff.position(0);
                                            if (buff.remaining() != target.write(buff)) {
                                                //broken pipe
                                                log("broken pipe");
                                                target.close();
                                            }
                                        }
                                    }
                                    at.byteBuffer.clear();
                                }
                            }
                            break;
                    }
                }
            }
            selector.selectedKeys().clear();
        }
    }

    private class Attachment {
        int id = 0;
        ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
        SocketChannel channel;

        private Attachment(SocketChannel channel) {
            this.channel = channel;
        }

        private int read() {
            int len;
            try {
                len = channel.read(byteBuffer);
            } catch (IOException e) {
                len = -1;
            }
            if (len < 0) {
                if (id > 0)
                    map.remove(id);
                close();
            }
            return len;
        }

        private int write(ByteBuffer src) {
            try {
                return channel.write(src);
            } catch (IOException e) {
                return -1;
            }
        }

        private void close() {
            try {
                channel.close();
            } catch (IOException ignore) {
            }
        }
    }

    private void log(String s, Object... objects) {
        System.out.printf(s + "\n", objects);
    }


    //==============================================================================================================
    private int testLimit = 5000;
    private int recvCount = 0;
    private int pushCount = 0;
    private Map<Integer, Integer> idPendMsg = new HashMap<>();
    private Map<Integer, Set<TestAttachment>> idAts = new HashMap<>();
    private List<TestAttachment> writableSet = new ArrayList<>();
    private Set<TestAttachment> pendingSet = new HashSet<>();


    private void test() {
        new Thread(() -> {
            recvCount = 0;
            pushCount = 0;
            idPendMsg.clear();
            writableSet.clear();
            pendingSet.clear();
            idAts.clear();
            SocketAddress serverAddress = new InetSocketAddress("192.168.0.119", 3889);
            Selector selector;
            try {
                selector = Selector.open();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            for (; ; ) {
                SocketChannel channel;
                try {
                    if (writableSet.size() + pendingSet.size() < testLimit * Math.random()) {
                        channel = SocketChannel.open();
                        channel.configureBlocking(false);
                        channel.connect(serverAddress);

                        TestAttachment at = new TestAttachment(channel);
                        pendingSet.add(at);
                        channel.register(selector, SelectionKey.OP_CONNECT, at);
                    }
                    selector.select();
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                Set<SelectionKey> keySet = selector.selectedKeys();
                for (SelectionKey key : keySet) {
                    if (!key.isValid())
                        continue;
                    TestAttachment at = (TestAttachment) key.attachment();
                    if (key.isConnectable()) {
                        try {
                            at.channel.finishConnect();
                        } catch (IOException e) {
                            log("connect failed");
                            pendingSet.remove(at);
                            continue;
                        }
                        key.interestOps(SelectionKey.OP_WRITE);
                    } else if (key.isWritable()) {
                        //connect success
                        pendingSet.remove(at);
                        writableSet.add(at);
                        key.interestOps(SelectionKey.OP_READ);
                    } else if (key.isReadable()) {
                        at.readable();
                    }
                    if (!writableSet.isEmpty() && keySet.size() < Math.random() * testLimit) {
                        writableSet.get((int) (Math.random() * writableSet.size())).writable();
                    }
                }
                keySet.clear();
                if (Math.random() < 0.01) {
                    log("pendingSet: " + pendingSet.size());
                    log("idAts: " + idAts.size());
                    int duplicate = 0;
                    for (Set<TestAttachment> set : idAts.values()) {
                        if (set.size() > 1)
                            ++duplicate;
                    }
                    log("duplicate: " + duplicate);

                    int pendMsg = 0;
                    for (Integer msgs : idPendMsg.values()) {
                        pendMsg += msgs;
                    }
                    log("pendMsg: " + pendMsg);
                    log("recvCount: " + recvCount);
                    log("pushCount: " + pushCount);
                }
            }
        }).start();
    }

    private class TestAttachment {
        int id = 0;
        ByteBuffer writeBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
        ByteBuffer readBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
        byte[] buff = new byte[MAX_MESSAGE_SIZE];
        SocketChannel channel;

        private TestAttachment(SocketChannel channel) {
            this.channel = channel;
        }

        private int bind(int id) {
            this.id = id;
            writeBuffer.putInt(0, 9);
            writeBuffer.put(4, (byte) 1);
            writeBuffer.putInt(5, id);
            writeBuffer.position(0);
            writeBuffer.limit(9);
            try {
                return channel.write(writeBuffer);
            } catch (IOException | NotYetConnectedException e) {
                e.printStackTrace();
                return -1;
            }
        }

        private void readable() {
            try {
                String r = read();
                if (r == null)
                    return;
                if (r.equals("shxhzhxx")) {
                    idPendMsg.put(id, idPendMsg.get(id) - 1);
                    //read success
                    ++recvCount;
                } else {
                    log("read wrong data");
                    //read failed
                }
            } catch (IOException e) {
                //read failed
                checkDuplicate("read");
            }
        }

        private void writable() {
            if (id == 0 && Math.random() * testLimit > idAts.size()) {//bind
                while (id == 0 || idAts.containsKey(id)) {
                    id = (int) (testLimit * Math.random());
                }
                if (bind(id) != 9) {
                    log("send bind failed");
                    return;
                }
                Set<TestAttachment> set = idAts.computeIfAbsent(id, k -> new HashSet<>());
                set.add(this);
            } else {//push
                byte[] data = "shxhzhxx".getBytes();
                int targetId = 0;
                while (targetId == 0 && !writableSet.isEmpty()) {//这里有可能死循环
                    targetId = writableSet.get((int) (Math.random() * writableSet.size())).id;
                }
                if (singlePush(targetId, data) != data.length + 9) {
                    //push failed
                    checkDuplicate("write");
                } else {
                    ++pushCount;
                    idPendMsg.merge(targetId, 1, (a, b) -> a + b);
                }
            }
        }

        private void checkDuplicate(String from) {
            Set<TestAttachment> set = idAts.get(id);
            if (set == null || !set.contains(this)) {
                log(from + " broken pipe: " + id);
            } else {
                set.remove(this);
                writableSet.remove(this);
                try {
                    channel.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        private String read() throws IOException {
            if (readBuffer.position() < 4) {
                readBuffer.limit(4);
                if (channel.read(readBuffer) < 0)
                    throw new IOException();
                if (readBuffer.position() < 4) {
                    //wait more data
                    return null;
                }
            }
            int len = readBuffer.getInt(0);
            readBuffer.limit(len);
            if (readBuffer.hasRemaining()) {
                if (channel.read(readBuffer) < 0)
                    throw new IOException();
                if (readBuffer.hasRemaining()) {
                    //wait more data.
                    return null;
                }
            }
            readBuffer.position(4);
            readBuffer.get(buff, 0, len - 4);
            readBuffer.clear();
            return new String(buff, 0, len - 4);
        }

        private int singlePush(int id, byte[] data) {
            int len = 9 + data.length;
            if (len > MAX_MESSAGE_SIZE)
                return -1;
            writeBuffer.limit(len);
            writeBuffer.position(0);
            writeBuffer.putInt(len);
            writeBuffer.put((byte) 2);
            writeBuffer.putInt(id);
            writeBuffer.put(data);
            writeBuffer.position(0);
            try {
                return channel.write(writeBuffer);
            } catch (IOException e) {
                return -1;
            }
        }
    }
}
