public class Main implements ICausalMulticast {
    private CausalMulticast causalMulticast;

    public Main() {
        causalMulticast = new CausalMulticast("239.255.255.245", 13087, this);
    }

    public void sendMulticastMessage(String msg) {
        causalMulticast.mcsend(msg, this);
    }

    @Override
    public void deliver(String msg) {
        System.out.println("Received message: " + msg);
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.sendMulticastMessage("Hello, world!");
    }
}
