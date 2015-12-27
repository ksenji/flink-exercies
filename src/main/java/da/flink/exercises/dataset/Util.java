package da.flink.exercises.dataset;

public class Util {

    private Util() {
    }

    public static String emailOfSender(String sender) {
        int indexOfLt = sender.indexOf('<') + 1;
        int indexOfgt = sender.indexOf('>', indexOfLt);
        return sender.substring(indexOfLt, indexOfgt);
    }
}
