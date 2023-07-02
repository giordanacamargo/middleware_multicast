import java.net.*;
import java.io.*;
import java.util.*;

public class CausalMulticast{
    //Constantes
    private static int timeout = 1000; //É um sistema síncrono, usando a medida de 1000ms para o timeout.
    private static int vectorClockSize = 10;
    
    
    
    static int process_vector_clock_id = 0;
    
    
    //private DatagramSocket socket;
    private String status;
    private MulticastSocket groupSocket;
    private InetAddress group;
    private int port;
    private int[] vectorClock;
    private int vectorClockIndex;    
    private int nextVectorClockIndex = -1;
    private int availableIndex = -1;
    private List<String> buffer;
    private ICausalMulticast client;
    private Map<ICausalMulticast, Integer> VectorClockDict;
    private Map<ICausalMulticast, Integer> SocketDict;    

    public CausalMulticast(String ip, int port, ICausalMulticast client) {
        try {
            this.status = "starting";
            //224.0.0.0 
            //through 
            //239.255.255.255
            //this.socket = new DatagramSocket(port, group);
            this.groupSocket = new MulticastSocket(port);
            this.group = InetAddress.getByName(ip);
            this.port = port;
            this.vectorClock = new int[vectorClockSize]; // Tamanho máximo para o vetor de relógios lógicos
            this.buffer = new ArrayList<>();
            this.client = client;
            this.VectorClockDict = new HashMap<ICausalMulticast, Integer>();
            groupSocket.joinGroup(group);
            
            startListening();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mcsend(String msg, ICausalMulticast client) {
        try {
            vectorClock[vectorClockIndex] += 1;
            client.deliver(msg);
            //VectorClockDict.replace(client, VectorClockDict.get(client)+1); // Incrementa o relógio lógico do processo atual
            String timestamp = buildTimestamp();

            String multicastMsg = "U" + ";l;" + timestamp + ";l;" + String.valueOf(vectorClockIndex) + ";l;" + msg;
            client.deliver(multicastMsg);
            client.deliver("Timestamp:" + timestamp);
            byte[] buf = multicastMsg.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, group, port);

            groupSocket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startListening() {
        Thread listenerThread = new Thread(() -> {
            try {                
                while (true) 
                {
                    byte[] buf;
                    String multicastMsg;
                    DatagramPacket packet;
                    if(this.status.equals("starting"))     
                    {              
                        this.status = "joining";    
                        multicastMsg = "C" + ";l;" + "JOINING";
                        buf = multicastMsg.getBytes();
                        packet = new DatagramPacket(buf, buf.length, group, port);
                        groupSocket.send(packet);

                        long startTime = System.currentTimeMillis();
                        long elapsedTime = 0;
                        while (elapsedTime < timeout) 
                        {
                            buf = new byte[1024];
                            packet = new DatagramPacket(buf, buf.length);
                            groupSocket.receive(packet);

                            String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                            processReceivedMessage(receivedMsg);
                            elapsedTime = System.currentTimeMillis()-startTime;
                        }  

                        if(this.availableIndex == -1)
                        {
                            this.vectorClockIndex = 0;
                            this.nextVectorClockIndex = 1;
                            this.status = "joined";
                        }

                        else if(this.availableIndex >= this.vectorClockSize)
                        {
                            client.deliver("O sistema atingiu seu limite de usuários.");
                            return;
                        }

                        else
                        {                          
                            this.vectorClockIndex = this.availableIndex;  
                            this.nextVectorClockIndex = vectorClockIndex + 1; 
                            this.status = "joined";
                        }
                        client.deliver("Expirou o tempo de aguardo de respostas.");
                        
                        if(this.status.equals("joined"))
                        {
                            client.deliver("Conectado, posição no Vetor de Relógios: " + String.valueOf(vectorClockIndex));
                            multicastMsg = "C" + ";l;" + "JOINED " + this.vectorClockIndex;
                            buf = multicastMsg.getBytes();
                            packet = new DatagramPacket(buf, buf.length, group, port);
                            groupSocket.send(packet);
                            this.status = "working";
                        }                        
                    }        
                    if(this.status == "working")
                    {
                        buf = new byte[1024];
                        packet = new DatagramPacket(buf, buf.length);
                        groupSocket.receive(packet);

                        String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                        processReceivedMessage(receivedMsg);
                    }
                }              
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        listenerThread.start();
    }

    private void processReceivedMessage(String receivedMsg) {
        String[] parts = receivedMsg.split(";l;");        
        String msgType = parts[0];

        if(msgType.equals("C"))
        {
            String msg = parts[1];
            String[] controlParts = msg.split(" ");
            switch(controlParts[0])
            {
                //JOINING IP
                case "JOINING":
                    if(this.status.equals("working"))
                    {
                        String multicastMsg = "C" + ";l;" + "ALREADY_JOINED " + this.nextVectorClockIndex;// Todos informam para o novo usuário qual é o próximo slot disponível.
                        byte[] buf = multicastMsg.getBytes();
                        DatagramPacket packet = new DatagramPacket(buf, buf.length, group, port);
                        try {                        
                            groupSocket.send(packet);
                        } catch (Exception e) {
                            // TODO: handle exception
                        }
                    }
                break;
                case "ALREADY_JOINED":
                    //Caso esteja aguardando respostas para saber onde poderá se posicionar no vetor de relógios
                    //Ele recebe - de todos as instâncias - qual é o lugar disponíve, detectando inconsistência caso eles discordem
                    //Dessa forma realizando um acordo do tipo consenso para definir onde será colocado no VC.
                    if(this.status.equals("joining"))
                    {
                        if(this.availableIndex == -1)
                        {
                            this.availableIndex = Integer.parseInt(controlParts[1]);
                        }
                        else if(this.availableIndex != Integer.parseInt(controlParts[1]))
                        {
                            client.deliver("Inconsistência informada no número de processos em ação. " + String.valueOf(this.availableIndex) + " " + controlParts[1]);
                        }
                        client.deliver("Estou me conectando e alguem mandou mensagem... "+ controlParts[1]);
                    }
                break;
                case "JOINED":
                    int indexVector = Integer.parseInt(controlParts[1]);
                    if(indexVector == this.nextVectorClockIndex)
                    {
                        this.nextVectorClockIndex = indexVector + 1;
                    }
                    client.deliver("Um novo usuário se conectou.");
                break;
                default:
                    System.out.println("Control message \"" + msg + "\" received.");
                break;
            }
            return;
        }

        //Só avalia mensagens comuns caso já tenha passado pelo processo de se posicionar devidamente no grupo
        if(!this.status.equals("working") || msgType.equals("C"))
        {
            return;
        }

        String timestamp = parts[1];
        int sender = Integer.parseInt(parts[2]);
        String msg = parts[3];

        updateVectorClock(timestamp, sender);
        buffer.add(receivedMsg);

        // Verifica se é possível entregar mensagens do buffer
        Iterator<String> iterator = buffer.iterator();
        while (iterator.hasNext()) {
            String bufferedMsg = iterator.next();
            String[] bufferedParts = bufferedMsg.split(";l;");
            String bufferedTimestamp = bufferedParts[1];
            int bufferedSender = Integer.parseInt(bufferedParts[2]);

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
