public class Main implements ICausalMulticast {
    private CausalMulticast causalMulticast;

    public Main() {
        causalMulticast = new CausalMulticast("localhost", 8080, this);
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
