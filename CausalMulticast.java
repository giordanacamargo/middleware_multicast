import java.net.*;
import java.io.*;
import java.util.*;

public class CausalMulticast{
    //Constantes
    private int timeout = 1000; //É um sistema síncrono, usando a medida de 1000ms para o timeout.
    private int groupEnterTimeout = 5000; //É um sistema síncrono, usando a medida de 1000ms para o timeout.
    private int vectorClockSize = 10;
    private int BASE_PORT = 20000;
    
    
    //private DatagramSocket socket;
    private String status;

    //Informações para a etapa de descoberta de novos membros
    private MulticastSocket GroupListener;
    private InetAddress GroupIP;
    private int GroupPort;


    private InetAddress ip;
    private int port;
    private DatagramSocket SelfSocket;
    private int vectorClockIndex;    
    private int nextVectorClockIndex = 0;
    private long groupEnterTimeCounter = 0; // Variavel que contabiliza o tempo de conexão inicial
    private long groupEnterStartTime = 0;   // Variável que armazena o momento inicial de tentiva de conexão
    private int availableIndex = -1;
    private List<String> buffer;
    private ICausalMulticast client;

    private int[] vectorClock;  //Vetor de Relógios
    private Map<Integer, InetAddress> IPs = new HashMap<>();          //Endereço acessadas pelo Identificador
    private Map<Integer, Integer> Ports = new HashMap<>();            //Endereço acessadas pelo Identificador

    public CausalMulticast(String ip, int port, ICausalMulticast client) {
        try {
            this.status = "starting";
            this.ip = InetAddress.getByName("127.0.0.1"); //ALTERAR
            this.port = 20000;
            //this.socket = new DatagramSocket(port, group);
            this.GroupIP = InetAddress.getByName(ip);
            this.GroupPort = port;
            this.GroupListener = new MulticastSocket(GroupPort);


            this.vectorClock = new int[vectorClockSize]; // Tamanho máximo para o vetor de relógios lógicos
            this.buffer = new ArrayList<>();
            this.client = client;
            GroupListener.setSoTimeout(this.timeout);
            GroupListener.joinGroup(this.GroupIP);
            
            startListening();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mcsend(String msg, ICausalMulticast client) {
        try {
            vectorClock[vectorClockIndex] += 1;
            client.deliver("(Envio proprio) " + msg);
            String timestamp = buildTimestamp();
            String multicastMsg = "U" + ";l;" + timestamp + ";l;" + String.valueOf(vectorClockIndex) + ";l;" + msg;
            byte[] buf = multicastMsg.getBytes();

            InetAddress temp_ip;
            Integer temp_port;
            //DatagramSocket temp_socket;

            for(int i = 0; i < this.nextVectorClockIndex; i++)
            {
                if(i == this.vectorClockIndex)
                    continue;

                temp_ip = IPs.get(i);
                temp_port = Ports.get(i);
                System.out.println("Enviando para o " + temp_ip + " : " + temp_port);
                //temp_socket = Sockets.get(i);
                DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName("localhost"), temp_port);
                try {
                    this.SelfSocket.send(packet);
                } catch (SocketException e) {
                    e.printStackTrace();
                    // TODO: handle exception
                }
                //temp_socket.send(packet);
                //temp_socket.receive(packet);
            }
            System.out.println("Mensagem enviada.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startListening() {

        //Responsável por Fazer a verificação de envio de mensagens no grupo de multicast.
        Thread GroupListenerThread = new Thread(() -> {
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
                        packet = new DatagramPacket(buf, buf.length, GroupIP, GroupPort);
                        GroupListener.send(packet);
                        this.groupEnterStartTime = System.currentTimeMillis();
                        try {
                            //Descarta a própria mensagem de Joining
                            GroupListener.receive(packet);     
                            String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                            client.deliver("Descartei essa: " + receivedMsg);
                        } catch (Exception e) {
                            // TODO: handle exception
                        }
                        
                    }
                    else if(this.status.equals("joining"))
                    {
                        this.groupEnterTimeCounter = System.currentTimeMillis() - this.groupEnterStartTime;
                        if(this.groupEnterTimeCounter < this.groupEnterTimeout)
                        {
                            buf = new byte[1024];
                            packet = new DatagramPacket(buf, buf.length);
                            try {
                                GroupListener.receive(packet); 
                                String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                                processReceivedMessage(receivedMsg);                                   
                            } catch (Exception e) {
                                System.out.println("Nenhuma resposta recebida nesta tentativa.");
                            } 
                        }
                        else
                        {                     
                            if(this.availableIndex != -1)
                            {
                                if(this.availableIndex >= this.vectorClockSize)
                                {
                                    client.deliver("O sistema atingiu seu limite de usuários.");
                                    return;
                                }
                                                                        
                                client.deliver("Me informaram que meu indíce deve ser: " + String.valueOf(this.availableIndex));       
                                this.vectorClockIndex = this.availableIndex;  
                                this.nextVectorClockIndex = vectorClockIndex + 1; 
                                this.port = BASE_PORT + this.vectorClockIndex;
                                this.SelfSocket = new DatagramSocket(this.port, this.ip);
                                this.IPs.put(this.vectorClockIndex, this.ip);
                                this.Ports.put(this.vectorClockIndex, this.port);
                                this.status = "joined";                                    
                            }
                            else
                            {
                                client.deliver("Ninguém respondeu a tentativa de conexão dentro do tempo específicado. Assumindo que o sistema ainda não possui usuários.");
                                this.vectorClockIndex = 0;
                                this.nextVectorClockIndex = 1;
                                this.port = BASE_PORT + this.vectorClockIndex;
                                this.SelfSocket = new DatagramSocket(this.port, this.ip);
                                this.IPs.put(this.vectorClockIndex, this.ip);
                                this.Ports.put(this.vectorClockIndex, this.port);
                                this.status = "joined";
                            }
                        }       
                    }                           
                    else if(this.status.equals("joined"))
                    {
                        client.deliver("Conectado, posição no Vetor de Relógios: " + String.valueOf(vectorClockIndex));
                        multicastMsg = "C" + ";l;" + "JOINED " + this.vectorClockIndex + " " + this.ip + " " + this.port;
                        buf = multicastMsg.getBytes();
                        packet = new DatagramPacket(buf, buf.length, this.GroupIP, this.GroupPort);
                        GroupListener.send(packet);
                        GroupListener.receive(packet);
                        this.status = "working";
                    }                   
                    else if(this.status == "working")
                    {
                        buf = new byte[1024];
                        packet = new DatagramPacket(buf, buf.length);
                        try {                            
                            GroupListener.receive(packet);
                            String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                            processReceivedMessage(receivedMsg);
                        } catch (Exception e) {
                            //System.out.println("Nenhuma mensagem recebida.");                            
                        }

                        StartSelfSocketListener();
                    }
                }              
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        GroupListenerThread.start();
    }

    //Thread que lê as mensagens recebidas no socket da porta unicast do processo.
    private void StartSelfSocketListener()
    {
        Thread SocketListenerThread = new Thread(() -> {
            try {                
                while (true) 
                {  
                    if(this.status == "working")
                    {
                        byte[] localbuf = new byte[1024];
                        DatagramPacket localPacket = new DatagramPacket(localbuf, localbuf.length);
                        try {                            
                            this.SelfSocket.receive(localPacket);
                            String threadMsg = new String(localPacket.getData(), 0, localPacket.getLength());
                            processUnicastMessage(threadMsg);
                        } catch (Exception e) {
                            System.out.println("Não recebi mensagem.");                            
                        }
                    }
                }              
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        SocketListenerThread.start();
    }

    //Mensagem recebida nas comunicações unicast pelo socket dedicado
    private void processUnicastMessage(String receivedMsg)
    {
        String[] parts = receivedMsg.split(";l;");    
        String msgType = parts[0];  
        String timestamp = parts[1];
        int sender = Integer.parseInt(parts[2]);
        String msg = parts[3];
        System.out.println("MENSAGEM: " + receivedMsg);

        updateVectorClock(timestamp, sender);
        buffer.add(receivedMsg);

        // Verifica se é possível entregar mensagens do buffer
        Iterator<String> iterator = buffer.iterator();
        while (iterator.hasNext()) 
        {
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
                        String multicastMsg = "C" + ";l;" + "ALREADY_JOINED " + this.nextVectorClockIndex + " " + this.vectorClockIndex + " " + this.ip + " " + this.port;// Todos informam para o novo usuário qual é o próximo slot disponível.
                        byte[] buf = multicastMsg.getBytes();
                        DatagramPacket packet = new DatagramPacket(buf, buf.length, GroupIP, GroupPort);
                        client.deliver("Foi solicitado o próximo indice disponível, informei que era o indice " + String.valueOf(this.nextVectorClockIndex) + ".");
                        try {                        
                            GroupListener.send(packet);
                            GroupListener.receive(packet);
                        } catch (Exception e) {
                            System.out.println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEESSSSSSSSSSSSSSSSSSSSSSTOOOOOOOOOOOOOOOUUUUUUUUUURRRRRRRRRRRRRRRRRRRROU");
                            // TODO: handle exception
                        }
                    }
                break;
                case "ALREADY_JOINED":
                    //Caso esteja aguardando respostas para saber onde poderá se posicionar no vetor de relógios
                    //Ele recebe - de todos as instâncias - qual é o lugar disponível, detectando inconsistência caso eles discordem
                    //Além disso, ele também recebe o endereço IP, a porta, e o indice do processo no VC.
                    //Dessa forma realizando um acordo do tipo consenso para definir onde será colocado no VC.
                    if(this.status.equals("joining"))
                    {
                        System.out.println("ESTOU ESPERANDO, RECEBI UM ALREADY JOINED. " + receivedMsg);
                        System.out.println("1: " + controlParts[1] + " 2: " + controlParts[2] + " 3: " + controlParts[3] + " 4: " + controlParts[4]);
                        int newAvailableIndex = Integer.parseInt(controlParts[1]);
                        Integer indexVector = Integer.parseInt(controlParts[2]);
                        InetAddress newIp;
                        Integer newPort;

                        try {                            
                            newIp = InetAddress.getLocalHost();//InetAddress.getByAddress(controlParts[3]);
                            newPort = Integer.parseInt(controlParts[4]);
                        } catch (UnknownHostException e) {
                            System.out.println("Erro em definir qual é o host partindo do nome do IP enviado. [233]");
                            return;
                        }

                        if(this.availableIndex == -1)
                        {
                            this.availableIndex = newAvailableIndex;
                        }
                        else if(this.availableIndex != newAvailableIndex)
                        {
                            client.deliver("Inconsistência informada no número de processos em ação. " + String.valueOf(this.availableIndex) + " " + controlParts[1]);
                        }
                        client.deliver("Estou me conectando e alguem mandou mensagem... "+ controlParts[1]);
                        
                        System.out.println("Salvando o endereço " + newIp + ":" + newPort + ", que é o processo de indice " + indexVector + "." );


                        IPs.put(indexVector, newIp);
                        Ports.put(indexVector, newPort);
                    }
                break;
                case "JOINED":
                    if(!this.status.equals("working"))
                    {
                        return;
                    }
                    Integer indexVector = Integer.parseInt(controlParts[1]);
                    InetAddress newIp;
                    Integer newPort;
                    
                    System.out.println("ESTOU ESPERANDO, RECEBI UM JOINED. " + receivedMsg);
                    try {
                        newIp = InetAddress.getLocalHost();//InetAddress.getByName(controlParts[2]);
                        newPort = Integer.parseInt(controlParts[3]);
                    } catch (UnknownHostException e) {
                        System.out.println("Erro em definir qual é o host partindo do nome do IP enviado. [296]");
                        return;
                    }

                    IPs.put(indexVector, newIp);
                    Ports.put(indexVector, newPort);
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
