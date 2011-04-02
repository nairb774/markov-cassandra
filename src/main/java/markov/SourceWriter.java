package markov;

import javax.inject.Inject;

import markov.MarkovProto.Source;

public class SourceWriter {
    private final CF<Source> sourceCf;

    @Inject
    public SourceWriter(CF<Source> sourceCf) {
        this.sourceCf = sourceCf;
    }

    public void write(final Source source, final RId id, final String string) {
        sourceCf.forKey(source).add(id, string);
        sourceCf.flush();
    }
}
