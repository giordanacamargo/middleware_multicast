import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Scanner;  // Import the Scanner class

public class Main implements ICausalMulticast {
    private CausalMulticast causalMulticast;

    public Main() {
        this.causalMulticast = new CausalMulticast("239.255.255.245", 13087, this);
    }

    public void sendMulticastMessage(String msg) {
        causalMulticast.mcsend(msg, this);
    }

    @Override
    public void deliver(String msg) {
        System.out.println("Received message: " + msg);
    }

    public static void main(String[] args) {
        Main Messenger = new Main();
        Messenger.sendMulticastMessage("Hello, world!");

        
        while (true) 
        {
            Scanner myObj = new Scanner(System.in);  // Create a Scanner object
            System.out.println("Insira a Mensagem: ");
            String msg = myObj.nextLine();  // Read user input
            Messenger.sendMulticastMessage(msg);

            /*System.out.println("A mensagem é: " + msg + ". Deseja atrasar o envio à alguma instância? (S ou N)");  // Output user input
            String r = myObj.nextLine();  // Read user inputt
            if(r.equals("S") || r.equals("s"))
            {
                System.out.println("Qual das instâncias deseja atrasar o envio?");
                String r2 = myObj.nextLine();  // Read user input
            }*/
        }   
    }
}
