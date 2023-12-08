package br.com.meslin.main.events;

public class TablePM25Event {
    private final int time;

    public TablePM25Event(int time) {
        this.time = time;
    }

    public int getTime() {
        return time;
    }
}
