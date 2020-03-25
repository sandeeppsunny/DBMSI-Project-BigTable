package diskmgr;

public class PCounter {
    public static int rcounter;
    public static int wcounter;

    public PCounter() {
        rcounter = 0;
        wcounter = 0;
    }

    public void initialize(){
        rcounter = 0;
        wcounter = 0;
    }

    public static void readIncrement() {
        rcounter++;
    }
    public static void writeIncrement() {
        wcounter++;
    }

    public int getRCounter(){
        return rcounter;
    }

    public int getWCounter(){
        return wcounter;
    }
}