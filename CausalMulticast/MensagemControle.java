package CausalMulticast;

import java.net.InetAddress;

public class MensagemControle {
    private static String controladorMensagensControle = "C";

    private static String separador = ";l;";

    private String msgType;

    private String msg;

    private String[] controlParts;

    public MensagemControle() {
    }

    public MensagemControle (String receivedMsg) {
        String[] parts = receivedMsg.split(separador);
        this.msgType = parts[0];
        this.msg = parts[1];
        this.controlParts = msg.split(" ");
    }

    public String montaMensagemControle (String tipo, InetAddress ip, int port, int vectorClockIndex) {
        return controladorMensagensControle + separador + tipo + " " + vectorClockIndex + " " + ip + " " + port;
    }

    public boolean isMensagemTipoControle () {
        return getMsgType().equals("C");
    }

    public boolean isMensagemJoining () {
        return this.controlParts[0].equals("JOINING");
    }

    public boolean isMensagemJoined () {
        return this.controlParts[0].equals("JOINED");
    }

    public boolean isMensagemAlreadyJoined () {
        return this.controlParts[0].equals("ALREADY_JOINED");
    }

    public static String getControladorMensagensControle() {
        return controladorMensagensControle;
    }

    public static void setControladorMensagensControle(String controladorMensagensControle) {
        MensagemControle.controladorMensagensControle = controladorMensagensControle;
    }

    public static String getSeparador() {
        return separador;
    }

    public static void setSeparador(String separador) {
        MensagemControle.separador = separador;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String[] getControlParts() {
        return controlParts;
    }

    public void setControlParts(String[] controlParts) {
        this.controlParts = controlParts;
    }
}
