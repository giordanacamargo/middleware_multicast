package CausalMulticast;

public class Mensagem {

    private String msgType;

    private String timestamp;

    private int sender;

    private String msg;

    private static String separador = ";l;";

    private static String controladorUnicast = "U";

    public Mensagem() {
    }

    public Mensagem (String receivedMsg) {
        String[] parts = receivedMsg.split(separador);

        this.msgType = parts[0];
        this.timestamp = parts[1];
        this.sender = Integer.parseInt(parts[2]);
        this.msg = parts[3];
    }

    public String montaMensagemDeliver (String mensagemASerEnviada, String timestamp, int vectorClockIndex) {
        return controladorUnicast + separador + timestamp + separador + vectorClockIndex + separador + mensagemASerEnviada;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getSender() {
        return sender;
    }

    public void setSender(int sender) {
        this.sender = sender;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public static String getSeparador() {
        return separador;
    }
}
