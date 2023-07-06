import CausalMulticast.CausalMulticast;
import CausalMulticast.ICausalMulticast;
import java.util.Scanner;

public class Main implements ICausalMulticast {
    private CausalMulticast causalMulticast;

    public Main() {
        this.causalMulticast = new CausalMulticast("239.255.255.245", 13087, this);
    }

    public void sendMulticastMessage (String msg) {
        causalMulticast.mcsend(msg, this);
    }

    @Override
    public void deliver (String msg) {
        System.out.println("Received message: " + msg);
    }

    public static void main(String[] args) {
        Main Messenger = new Main();
        //Messenger.sendMulticastMessage("Hello, world!");

        
        while (true) {
            System.out.println("Caso deseje atrasar o recebimento em algum processo, insira a mensagem seguida por |n para atrasar o recebimento da mensagem no indice n.");
            Scanner myObj = new Scanner(System.in);
            String msg = myObj.nextLine();
            Messenger.sendMulticastMessage(msg);
        }   
    }
}
