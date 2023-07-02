import java.net.*;
import java.io.*;
import java.util.*;

public class CausalMulticast {
    private MulticastSocket socket;
    private InetAddress group;
    private int port;
    private int[] vectorClock;
    private List<String> buffer;
    private ICausalMulticast client;

    public CausalMulticast(String ip, int port, ICausalMulticast client) {
        try {
            this.socket = new MulticastSocket(port);
            this.group = InetAddress.getByName(ip);
            this.port = port;
            this.vectorClock = new int[10]; // Tamanho máximo para o vetor de relógios lógicos
            this.buffer = new ArrayList<>();
            this.client = client;

            socket.joinGroup(group);
            startListening();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mcsend(String msg, ICausalMulticast client) {
        try {
            vectorClock[port]++; // Incrementa o relógio lógico do processo atual
            String timestamp = buildTimestamp();

            String multicastMsg = timestamp + " " + port + " " + msg;
            byte[] buf = multicastMsg.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, port);

            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startListening() {
        Thread listenerThread = new Thread(() -> {
            try {
                while (true) {
                    byte[] buf = new byte[1024];
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);

                    String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                    processReceivedMessage(receivedMsg);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        listenerThread.start();
    }

    private void processReceivedMessage(String receivedMsg) {
        String[] parts = receivedMsg.split(" ");
        String timestamp = parts[0];
        int sender = Integer.parseInt(parts[1]);
        String msg = parts[2];

        updateVectorClock(timestamp, sender);
        buffer.add(receivedMsg);

        // Verifica se é possível entregar mensagens do buffer
        Iterator<String> iterator = buffer.iterator();
        while (iterator.hasNext()) {
            String bufferedMsg = iterator.next();
            String[] bufferedParts = bufferedMsg.split(" ");
            String bufferedTimestamp = bufferedParts[0];
            int bufferedSender = Integer.parseInt(bufferedParts[1]);

            if (bufferedSender != port && isCausallyReady(bufferedTimestamp)) {
                client.deliver(bufferedMsg);
                iterator.remove();
            }
        }
    }

    private void updateVectorClock(String timestamp, int sender) {
        String[] timestampParts = timestamp.split(",");
        int[] receivedClock = new int[timestampParts.length];

        for (int i = 0; i < timestampParts.length; i++) {
            receivedClock[i] = Integer.parseInt(timestampParts[i]);
        }

        // Atualiza o vetor de relógios lógicos com o máximo entre os relógios atuais e o recebido
        for (int i = 0; i < vectorClock.length; i++) {
            vectorClock[i] = Math.max(vectorClock[i], receivedClock[i]);
        }

        vectorClock[sender]++; // Incrementa o relógio lógico do remetente
    }

    private boolean isCausallyReady(String timestamp) {
        String[] timestampParts = timestamp.split(",");
        int[] receivedClock = new int[timestampParts.length];

        for (int i = 0; i < timestampParts.length; i++) {
            receivedClock[i] = Integer.parseInt(timestampParts[i]);
        }

        // Verifica se todas as entradas do vetor de relógios lógicos são menores ou iguais às correspondentes do vetor recebido
        for (int i = 0; i < vectorClock.length; i++) {
            if (vectorClock[i] < receivedClock[i]) {
                return false;
            }
        }

        return true;
    }

    private String buildTimestamp() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < vectorClock.length; i++) {
            sb.append(vectorClock[i]);
            if (i < vectorClock.length - 1) {
                sb.append(",");
            }
        }

        return sb.toString();
    }
}
