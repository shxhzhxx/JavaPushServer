package shxhzhxx;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class PushServer {
    public static void main(String[] args) {
        PushServer server = new PushServer(3889, 1024);
        server.test();
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
                        if (!at.read()) {
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
                    if (!at.read()) {
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
                        case 0://echo
                            at.byteBuffer.putInt(1, len - 1);
                            at.byteBuffer.position(1);
                            if (at.write(at.byteBuffer)) {
                                at.byteBuffer.clear();
                            } else {
                                //broken pipe
                            }
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
                                    if (!target.write(at.byteBuffer)) {
                                        //broken pipe
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
                                int headerLen = 9 + num * 4;
                                if (len < headerLen) {
                                    log("multi push invalid param, len(%d)<9+num(%d)*4", len, num);
                                    if (at.id > 0)
                                        map.remove(at.id);
                                    at.close();
                                } else {
                                    at.byteBuffer.position(headerLen);
                                    buff.clear();
                                    buff.putInt(len - headerLen + 4);
                                    buff.put(at.byteBuffer);
                                    buff.limit(len - headerLen + 4);
                                    for (int i = 0; i < num; ++i) {
                                        Attachment target = map.get(at.byteBuffer.getInt(9 + 4 * i));
                                        if (target != null) {
                                            buff.position(0);
                                            if (!target.write(buff)) {
                                                //broken pipe
                                            }
                                        }
                                    }
                                    at.byteBuffer.clear();
                                }
                            }
                            break;
                        case 4://get buffer size in bytes
                            if (len != 5) {
                                log("get buffer size invalid param, len(%d)!=5", len);
                                if (at.id > 0)
                                    map.remove(at.id);
                                at.close();
                            } else {
                                at.byteBuffer.limit(8).position(0);
                                at.byteBuffer.putInt(8);
                                at.byteBuffer.putInt(MAX_MESSAGE_SIZE);
                                at.byteBuffer.position(0);
                                if (at.write(at.byteBuffer)) {
                                    at.byteBuffer.clear();
                                } else {
                                    //broken pipe
                                }
                            }
                            break;
                        case 5://get ip
                            InetAddress inetAddress = at.channel.socket().getInetAddress();
                            if (len != 5 || inetAddress == null) {
                                log("get ip invalid param, len(%d)!=5 or inetAddress == null", len);
                                if (at.id > 0)
                                    map.remove(at.id);
                                at.close();
                            } else {
                                byte[] address = inetAddress.getHostAddress().getBytes();
                                at.byteBuffer.limit(address.length + 4).position(0);
                                at.byteBuffer.putInt(address.length + 4);
                                at.byteBuffer.put(address);
                                at.byteBuffer.position(0);
                                if (at.write(at.byteBuffer)) {
                                    at.byteBuffer.clear();
                                } else {
                                    //broken pipe
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

        private boolean read() {
            int len;
            try {
                len = channel.read(byteBuffer);
            } catch (IOException e) {
                len = -1;
            }
            if (len >= 0) {
                return true;
            }
            if (id > 0)
                map.remove(id);
            close();
            return false;
        }

        private boolean write(ByteBuffer src) {
            int remaining = src.remaining();
            int len;
            try {
                len = channel.write(src);
            } catch (IOException e) {
                len = -1;
            }
            if (remaining == len) {
                return true;
            }
            if (id > 0)
                map.remove(id);
            close();
            return false;
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
    private final boolean DUPLICATE_ID = false;
    private final int TEST_LIMIT = 5000;
    private int socketCount=0;
    private int pushCount = 0;
    private int recvSuccess = 0;
    private int recvWrong = 0;
    private Map<Integer, Stack<TestAttachment>> idAts = new HashMap<>();
    private List<TestAttachment> writableSet = new ArrayList<>();
    private Set<TestAttachment> pendingSet = new HashSet<>();


    private void test() {
        new Thread(() -> {
            socketCount = 0;
            recvSuccess = 0;
            pushCount = 0;
            recvWrong = 0;
            writableSet.clear();
            pendingSet.clear();
            idAts.clear();
            report();
            SocketAddress serverAddress = new InetSocketAddress("39.106.101.63", 3889);
            Selector selector;
            try {
                selector = Selector.open();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }

            for (; ; ) {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                SocketChannel channel;
                try {
                    if (writableSet.size() + pendingSet.size() < TEST_LIMIT * Math.random()) {
                        channel = SocketChannel.open();
                        channel.socket().setReuseAddress(true);
                        channel.configureBlocking(false);
                        channel.connect(serverAddress);

                        TestAttachment at = new TestAttachment(channel);
                        synchronized (PushServer.this) {
                            pendingSet.add(at);
                        }
                        channel.register(selector, SelectionKey.OP_CONNECT, at);
                    }
                    selector.select();
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

                synchronized (PushServer.this) {
                    Set<SelectionKey> keySet = selector.selectedKeys();
                    for (SelectionKey key : keySet) {
                        if (!key.isValid()) {
                            continue;
                        }
                        TestAttachment at = (TestAttachment) key.attachment();
                        if (key.isConnectable()) {
                            try {
                                at.channel.finishConnect();
                            } catch (IOException e) {
                                log("connect failed");
                                pendingSet.remove(at);
                                at.clear();
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
                        if (!writableSet.isEmpty() && keySet.size() < Math.random() * TEST_LIMIT) {
                            writableSet.get((int) (Math.random() * writableSet.size())).writable();
                        }
                    }
                    keySet.clear();
                }
            }
        }).start();
    }
//
//    /**
//     * 测试结果说明：
//     * pendingSet记录发起但没被响应的连接数，稳定态时应该是0
//     * idAts记录当前被绑定的id总数
//     * duplicate记录id重复但没有被断开的连接数，稳定态时应该是0
//     * pushCount记录总推送数
//     * recvSuccess记录收到成功推送的数量
//     * recvWrong记录收到错误推送的数量（A的数据推给了B）
//     * <p>
//     * 由于（不可靠）推送机制的设计，（recvSuccess+recvWrong）通常小于pushCount，也就是说有数据丢失。
//     * 原因是bind操作没有返回成功信息，所以测试程序发起绑定后推送服务器绑定成功前这段时间对这个id的推送数据会丢失（如果有重复id，这段时间的数据可能会推送错目标）
//     * 实际使用时还要面临意外断线的情况，所以应该保证id不会重复，且对每一个推送在业务层验证送达。
//     */
    private void report() {
        new Thread(() -> {
            for (; ; ) {
                synchronized (PushServer.this) {
                    log("pendingSet: " + pendingSet.size());
                    log("idAts: " + idAts.size());
                    int duplicate = 0;
                    for (Stack<TestAttachment> set : idAts.values()) {
                        if (set.size() > 1)
                            ++duplicate;
                    }
                    log("duplicate: " + duplicate);

                    log("pushCount: " + pushCount);
                    log("recvSuccess: " + recvSuccess);
                    log("recvWrong: " + recvWrong);
                    log("socketCount: " + socketCount);
                    log("\n");
                }
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
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
        final String msg = String.valueOf(hashCode());

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
                if (r.equals(msg)) {
                    //read success
                    ++recvSuccess;
                } else {
                    //read failed
                    ++recvWrong;
                }
            } catch (IOException e) {
                //read failed
                clear();
            }
        }

        private void writable() {
            if (writableSet.size() > 10 * TEST_LIMIT * Math.random()) {
                clear();
            } else if (id == 0 && Math.random() * TEST_LIMIT > idAts.size()) {//bind
                while (id == 0 || (!DUPLICATE_ID && idAts.containsKey(id))) {
                    id = (int) (TEST_LIMIT * Math.random());
                }
                if (bind(id) != 9) {
                    log("send bind failed");
                    return;
                }
                Stack<TestAttachment> stack = idAts.computeIfAbsent(id, k -> new Stack<>());
                stack.push(this);
            } else {//push
                int targetId = 0;
                while (targetId == 0 && !writableSet.isEmpty()) {//这里有可能死循环
                    targetId = writableSet.get((int) (Math.random() * writableSet.size())).id;
                }
                byte[] data = idAts.get(targetId).peek().msg.getBytes();
                if (singlePush(targetId, data) != data.length + 9) {
                    //push failed
                    clear();
                } else {
                    ++pushCount;
                }
            }
        }

        private void clear() {
            Stack<TestAttachment> stack = idAts.get(id);
            if (stack == null || !stack.contains(this)) {
                if (id != 0)
                    log(" stack == null || !stack.contains(this):  " + id);
            } else {
                stack.remove(this);
                if (stack.isEmpty())
                    idAts.remove(id);
            }
            writableSet.remove(this);
            try {
                channel.close();
            } catch (IOException e1) {
                e1.printStackTrace();
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
