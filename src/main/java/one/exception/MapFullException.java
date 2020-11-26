package one.exception;

/**
 * Honestly I don't know is this is exception so necessary, but I suppose map needs signalise when it's full
 */
public class MapFullException extends Exception {
    public MapFullException(String message) {
        super(message);
    }

    //To remove stack trace creation overhead
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
