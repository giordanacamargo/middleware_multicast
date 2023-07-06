package CausalMulticast;

import java.net.*;
import java.io.*;
import java.util.*;

public class CausalMulticast{

    //Constantes
    private int timeout = 1000;             //É um sistema síncrono, usando a medida de 1000ms para o timeout.
    private int groupEnterTimeout = 2500;   //Define quanto tempo ele aguarda as respostas no grupo quando está iniciando a conexão.
    private int vectorClockSize = 10;       //Define o número de indices do VectorClock.
    private int BASE_PORT = 2000;           //Define
    private int delayTimeMillis = 5000;

    
    private Estados status;

    //Informações para a etapa de descoberta de novos membros
    private MulticastSocket GroupListener;
    private InetAddress GroupIP;
    private int GroupPort;

    private int delayAt = -1;

    private Mensagem mensagem = new Mensagem();
    private MensagemControle mensagemControle = new MensagemControle();
    private InetAddress ip;
    private int port;
    private DatagramSocket SelfSocket;
    private int vectorClockIndex;
    private int nextVectorClockIndex = 0;
    private long groupEnterTimeCounter = 0; // Variavel que contabiliza o tempo de conexão inicial
    private long groupEnterStartTime = 0;   // Variável que armazena o momento inicial de tentiva de conexão
    private int groupConnectTryCount = 1;
    private int availableIndex = -1;
    private List<Mensagem> buffer;
    private ICausalMulticast client;

    private int[] vectorClock;  //Vetor de Relógios

    //Vetor de Relógios de todos os processos (considera o número máximo de processos)
    private int[][] vectorClockMatrix = new int[vectorClockSize][vectorClockSize];

    private Map<Integer, InetAddress> IPs = new HashMap<>();          //Endereço acessadas pelo Identificador
    private Map<Integer, Integer> Ports = new HashMap<>();            //Endereço acessadas pelo Identificador

    public CausalMulticast (String ip, int port, ICausalMulticast client) {
        try {
            // A máquina é iniciada no estado STARTING
            this.status = Estados.STARTING;
            this.ip = InetAddress.getByName("localhost"); //ALTERAR
            this.port = 20000;
            //this.socket = new DatagramSocket(port, group);
            this.GroupIP = InetAddress.getByName(ip);
            this.GroupPort = port;
            this.GroupListener = new MulticastSocket(GroupPort);
            GroupListener.setSoTimeout(this.timeout);
            GroupListener.joinGroup(this.GroupIP);

            this.vectorClock = new int[vectorClockSize]; // Tamanho máximo para o vetor de relógios lógicos
            this.buffer = new ArrayList<>();
            this.client = client;

            startGroupListening();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Envia uma mensagem multicast causal para um cliente específico.
     *
     * @param msg: A mensagem a ser enviada.
     * @param client: O cliente ICausalMulticast para o qual a mensagem será enviada.
     */
    public void mcsend (String msg, ICausalMulticast client) {
        try {
            if(msg.contains("|"))
            {
                String[] splittedMsg = msg.split("\\|");
                this.delayAt = Integer.parseInt(splittedMsg[1]);
                msg = splittedMsg[0];
            }

            // O processo emissor anexa seu vetor de relógios lógicos à mensagem antes de enviá-la.
            String timestamp = buildTimestamp();
            String unicastMsg = mensagem.montaMensagemDeliver(msg, timestamp, this.vectorClockIndex);

            byte[] buf = unicastMsg.getBytes();
            InetAddress temp_ip;
            Integer temp_port;

            for (int i = 0; i < this.nextVectorClockIndex; i++) {
                if (i == this.vectorClockIndex || i == this.delayAt)
                    continue;

                temp_ip = IPs.get(i);
                temp_port = Ports.get(i);
                //System.out.println("Enviando para o " + temp_ip + " : " + temp_port);
                DatagramPacket packet = new DatagramPacket(buf, buf.length, InetAddress.getByName("localhost"), temp_port);
                try {
                    this.SelfSocket.send(packet);
                } catch (SocketException e) {
                    e.printStackTrace();
                    return;
                }
            }
            
            if(this.delayAt != -1)
            {     
                Thread DelayThread = new Thread(() -> {
                    try {                
                        while (true) {                       
                            int shouldDelay = this.delayAt;
                            this.delayAt = -1;
                            
                            InetAddress temp_ip_delay = InetAddress.getByName("localhost");//IPs.get(shouldDelay);
                            int temp_port_delay = Ports.get(shouldDelay);
                            //System.out.println("Enviando para o " + temp_ip + " : " + temp_port);
                            DatagramPacket packet = new DatagramPacket(buf, buf.length, temp_ip_delay, temp_port_delay);
                            try {
                                Thread.sleep(delayTimeMillis);
                                this.SelfSocket.send(packet);
                            } catch (Exception e) {
                                e.printStackTrace();
                                return;
                            }
                        }              
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                DelayThread.start();
            }
            System.out.println("Mensagem enviada.");
            // O processo emissor atualiza seu próprio vetor de relógios lógicos incrementando o valor correspondente ao seu índice.
            vectorClock[vectorClockIndex] += 1;

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Inicia a escuta do grupo.
     * Responsável por fazer a verificação de envio de mensagens no grupo de multicast.
     * A função é executada em uma thread separada para receber e processar mensagens do grupo multicast.
     *
     */
    private void startGroupListening() {
        Thread GroupListenerThread = new Thread(() -> {
            try {
                while (true) {
                    byte[] buf;
                    String multicastMsg;
                    DatagramPacket packet;
                    if (this.status == Estados.STARTING) {
                        this.status = Estados.JOINING;
                        multicastMsg = "C" + Mensagem.getSeparador() + "JOINING";
                        buf = multicastMsg.getBytes();
                        packet = new DatagramPacket(buf, buf.length, GroupIP, GroupPort);
                        GroupListener.send(packet);
                        this.groupEnterStartTime = System.currentTimeMillis();
                        try {
                            //Descarta a própria mensagem de Joining
                            GroupListener.receive(packet);
                            String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                        } catch (Exception e) {
                            // TODO: handle exception
                        }

                    } else if (this.status == Estados.JOINING) {
                        this.groupEnterTimeCounter = System.currentTimeMillis() - this.groupEnterStartTime;
                        if (this.groupEnterTimeCounter < this.groupEnterTimeout) {
                            buf = new byte[1024];
                            packet = new DatagramPacket(buf, buf.length);
                            try {
                                GroupListener.receive(packet);
                                String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                                processGroupMessage(receivedMsg);
                            } catch (Exception e) {
                                //System.out.println("SISTEMA: Nenhuma resposta recebida nesta tentativa. ["+String.valueOf(this.groupConnectTryCount)+"]");
                                this.groupConnectTryCount += 1;
                            }
                        } else {
                            if (this.availableIndex != -1) {
                                if (this.availableIndex >= this.vectorClockSize) {
                                    System.out.println("SISTEMA: O sistema atingiu seu limite de usuários.");
                                    return;
                                }
                                System.out.println("SISTEMA: Conectado, indice atribuído: " + this.availableIndex);
                                this.vectorClockIndex = this.availableIndex;
                                this.nextVectorClockIndex = vectorClockIndex + 1;
                            } else {
                                System.out.println("SISTEMA: Ninguém respondeu a tentativa de conexão dentro do tempo específicado. Assumindo que o sistema ainda não possui usuários.");
                                this.vectorClockIndex = 0;
                                this.nextVectorClockIndex = 1;
                            }

                            this.port = BASE_PORT + this.vectorClockIndex;
                            this.SelfSocket = new DatagramSocket(this.port, this.ip);
                            this.IPs.put(this.vectorClockIndex, this.ip);
                            this.Ports.put(this.vectorClockIndex, this.port);
                            this.status = Estados.JOINED;
                        }
                    } else if (this.status == Estados.JOINED) {                        
                        System.out.println("SISTEMA: Conectado, posição no Vetor de Relógios: " + vectorClockIndex);
                        multicastMsg = mensagemControle.montaMensagemControle("JOINED", this.ip, this.port, this.vectorClockIndex);
                        System.out.println(multicastMsg);
                        buf = multicastMsg.getBytes();
                        packet = new DatagramPacket(buf, buf.length, this.GroupIP, this.GroupPort);
                        GroupListener.send(packet);
                        GroupListener.receive(packet);
                        this.status = Estados.WORKING;

                    } else if (this.status == Estados.WORKING) {
                        buf = new byte[1024];
                        packet = new DatagramPacket(buf, buf.length);
                        try {                            
                            GroupListener.receive(packet);
                            String receivedMsg = new String(packet.getData(), 0, packet.getLength());
                            processGroupMessage(receivedMsg);
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

    /**
     * Inicia a escuta do socket local para receber mensagens unicast.
     * Esta função é executada em uma thread separada para permitir a escuta contínua do socket.
     * Ela processa as mensagens unicast recebidas e as encaminha para o método de processamento apropriado.
     */
    private void StartSelfSocketListener() {
        Thread SocketListenerThread = new Thread(() -> {
            try {                
                while (true) {
                    if (status == Estados.WORKING) {
                        byte[] localbuf = new byte[1024];
                        DatagramPacket localPacket = new DatagramPacket(localbuf, localbuf.length);
                        try {                            
                            this.SelfSocket.receive(localPacket);
                            String threadMsg = new String(localPacket.getData(), 0, localPacket.getLength());
                            processUnicastMessage(threadMsg);
                        } catch (Exception e) {
                            //System.out.println("SISTEMA: Não recebi mensagem.");                            
                        }
                    }
                }              
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        SocketListenerThread.start();
    }

    /**
     * Processa uma mensagem unicast recebida.
     * Extrai as informações relevantes da mensagem e realiza as operações necessárias,
     * como atualização do vector clock e adição da mensagem ao buffer de mensagens.
     *
     * Ao receber uma mensagem, o processo receptor atualiza seu próprio vetor de relógios lógicos para cada posição,
     * levando em consideração o valor máximo entre o valor atual e o valor recebido na mensagem.
     * Antes de entregar uma mensagem recebida à aplicação, o processo receptor verifica se todas as dependências causais foram satisfeitas,
     * ou seja, se o vetor de relógios lógicos recebido é menor ou igual ao vetor de relógios lógicos local em todas as posições, exceto na posição correspondente ao processo emissor.
     * Se todas as dependências causais forem satisfeitas, o processo receptor entrega a mensagem à aplicação.
     * Caso contrário, a mensagem é armazenada em um buffer até que todas as dependências sejam atendidas.
     * Periodicamente, o processo verifica o buffer de mensagens armazenadas e verifica se alguma mensagem pode ser entregue com base na ordem causal.
     *
     *  @param receivedMsg A mensagem unicast recebida pelo socket dedicado.
     */
    private void processUnicastMessage (String receivedMsg) {
        Mensagem mensagemRecebida = new Mensagem(receivedMsg);
        //deposit(m)
        buffer.add(mensagemRecebida);
        // MCi[j][*]  m.VC
        updateVectorClock(mensagemRecebida);

        //if i ≠ j then MCi[i][j]  MCi[i][j]+1
        if (vectorClockIndex != mensagemRecebida.getSender()) {
            vectorClockMatrix[vectorClockIndex][mensagemRecebida.getSender()]++;
            //deliver msg to the upper layer % evento de entrega
            client.deliver(mensagemRecebida.getMsg());
        }

        // Verifica se é possível entregar mensagens do buffer
        Iterator<Mensagem> iterator = buffer.iterator();
        //when (existe msg no bufferi AND msg.VC[msg.sender] ≤ min1≤x≤n(MCi[x][msg.sender])
        while (iterator.hasNext()) {
            Mensagem bufferedMsg = iterator.next();
            if (isCausallyReady(bufferedMsg)) {
                // discart(msg)
                iterator.remove();
            }
        }
    }

    private void processGroupMessage(String receivedMsg) {
        this.mensagemControle = new MensagemControle(receivedMsg);
        if (this.mensagemControle.isMensagemTipoControle()) {
            if (this.mensagemControle.isMensagemJoining()){
                if (this.status == Estados.WORKING) {
                    // Todos informam para o novo usuário qual é o próximo slot disponível.
                    String multicastMsg = "C" + mensagemControle.getSeparador() + "ALREADY_JOINED " + this.nextVectorClockIndex + " " + this.vectorClockIndex + " " + this.ip + " " + this.port;
                    byte[] buf = multicastMsg.getBytes();
                    DatagramPacket packet = new DatagramPacket(buf, buf.length, GroupIP, GroupPort);
                    System.out.println("SISTEMA: Foi solicitado o próximo indice disponível, informei que era o indice " + this.nextVectorClockIndex + ".");
                    try {
                        GroupListener.send(packet);
                        GroupListener.receive(packet);
                    } catch (Exception e) {
                        System.out.println("SISTEMA: Houve um estouro ao enviar informações sobre novos slots disponíveis.");
                    }
                }

            } else if (this.mensagemControle.isMensagemAlreadyJoined()) {
                //Caso esteja aguardando respostas para saber onde poderá se posicionar no vetor de relógios
                //Ele recebe - de todos as instâncias - qual é o lugar disponível, detectando inconsistência caso eles discordem
                //Além disso, ele também recebe o endereço IP, a porta, e o indice do processo no VC.
                //Dessa forma realizando um acordo do tipo consenso para definir onde será colocado no VC.
                if (this.status == Estados.JOINING) {
                    this.mensagemControle = new MensagemControle(receivedMsg);
                    String controlParts[] = this.mensagemControle.getControlParts();
                    int newAvailableIndex = Integer.parseInt(controlParts[1]);
                    Integer indexVector = Integer.parseInt(controlParts[2]);
                    InetAddress newIp;
                    Integer newPort;

                    try {
                        //REVER CASO DO IP
                        newIp = InetAddress.getLocalHost();//InetAddress.getByAddress(controlParts[3]);
                        newPort = Integer.parseInt(controlParts[4]);
                    } catch (UnknownHostException e) {
                        System.out.println("SISTEMA: Erro em definir qual é o host partindo do nome do IP enviado. [233]");
                        return;
                    }

                    if (this.availableIndex == -1) {
                        this.availableIndex = newAvailableIndex;
                    } else if (this.availableIndex != newAvailableIndex) {
                        System.out.println("SISTEMA: Inconsistência informada no indice disponível do vetor de relógios por parte de dois processos diferentes. " + String.valueOf(this.availableIndex) + " " + controlParts[1]);
                    }

                    System.out.println("SISTEMA: Novo endereco " + newIp + ":" + newPort + ", de posicao " + indexVector + " no vector clock." );

                    IPs.put(indexVector, newIp);
                    Ports.put(indexVector, newPort);
                }
            } else if (this.mensagemControle.isMensagemJoined()) {
                if (!(this.status == Estados.WORKING)) {
                    return;
                }

                String controlParts[] = mensagemControle.getControlParts();
                Integer indexVector = Integer.parseInt(controlParts[1]);
                InetAddress newIp;
                Integer newPort;

                //System.out.println("ESTOU ESPERANDO, RECEBI UM JOINED. " + receivedMsg);
                try {
                    //PEGA DIRETO O LOCALHOST
                    newIp = InetAddress.getLocalHost();//InetAddress.getByName(controlParts[2]);
                    newPort = Integer.parseInt(controlParts[3]);
                } catch (UnknownHostException e) {
                    System.out.println("Erro em definir qual é o host partindo do nome do IP enviado. [296]");
                    return;
                }

                IPs.put(indexVector, newIp);
                Ports.put(indexVector, newPort);
                if (indexVector == this.nextVectorClockIndex) {
                    this.nextVectorClockIndex = indexVector + 1;
                }
                System.out.println("SISTEMA: Um novo usuario se conectou. [VCIndex = " + indexVector + "]");
            } else {
                System.out.println("SISTEMA: Control message \"" + receivedMsg + "\" received.");
            }
        }
    }

    private void updateVectorClock(Mensagem mensagemRecebida) {

        String[] timestampParts = mensagemRecebida.getTimestamp().split(",");
        int[] receivedClock = new int[timestampParts.length];

        for (int i = 0; i < timestampParts.length; i++) {
            receivedClock[i] = Integer.parseInt(timestampParts[i]);
        }

        // Atualiza a matriz de relógios com o máximo entre os relógios atuais e os recebidos
        for (int j = 0; j < vectorClockSize; j++) {
            vectorClockMatrix[vectorClockIndex][j] = Math.max(vectorClockMatrix[vectorClockIndex][j], receivedClock[j]);
        }

    }

    private boolean isCausallyReady(Mensagem msg) {
        String[] timestampParts =  msg.getTimestamp().split(",");
        int[] receivedClock = new int[timestampParts.length];

        // Converte a string em vetor de int
        for (int i = 0; i < timestampParts.length; i++) {
            receivedClock[i] = Integer.parseInt(timestampParts[i]);
        }
        
        for (int j = 0; j < vectorClockSize; j++) {
            if (vectorClockMatrix[vectorClockIndex][j] > receivedClock[j]) {
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
