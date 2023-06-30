/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.shell;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.jline.builtins.Completers;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.ArgumentCompleter;

/**
 * Same as {@link ArgumentCompleter} but with more strict validation for options.
 */
public class OptionStrictArgumentCompleter implements Completer {

    private final List<Completer> completers = new ArrayList<>();
    /**
     * Create a new completer.
     *
     * @param completers    The embedded completers
     */
    public OptionStrictArgumentCompleter(final Collection<Completer> completers) {
        Objects.requireNonNull(completers);
        this.completers.addAll(completers);
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        Objects.requireNonNull(line);
        Objects.requireNonNull(candidates);

        if (line.wordIndex() < 0) {
            return;
        }

        Completer completer;

        // if we are beyond the end of the completers, just use the last one
        if (line.wordIndex() >= completers.size()) {
            completer = completers.get(completers.size() - 1);
        } else {
            completer = completers.get(line.wordIndex());
        }


        // ensure that all the previous completers are successful
        // before allowing this completer to pass (only if strict).
        for (int i = 0; i < line.wordIndex(); i++) {
            int idx = i >= completers.size() ? (completers.size() - 1) : i;
            Completer sub = completers.get(idx);

            List<? extends CharSequence> args = line.words();
            String arg = (args == null || i >= args.size()) ? "" : args.get(i).toString();

            List<Candidate> subCandidates = new LinkedList<>();
            /*
             * This is the part that differs from the original ArgumentCompleter.
             * It matches only if there's an actual option.
             * The implementation of OptionCompleter will return the same candidate even if it is
             * not part of the options set because options are not required.
             */
            if (sub instanceof Completers.OptionCompleter) {
                if (arg.startsWith("-")) {
                    sub.complete(reader, new ArgumentCompleter.ArgumentLine(arg, arg.length()), subCandidates);
                } else {
                    return;
                }
            } else {
                sub.complete(reader, new ArgumentCompleter.ArgumentLine(arg, arg.length()), subCandidates);
            }


            boolean found = false;
            for (Candidate cand : subCandidates) {
                if (cand.value().equals(arg)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return;
            }
        }
        completer.complete(reader, line, candidates);
    }

}
