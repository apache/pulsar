package org.apache.pulsar.common.policies.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EntryFilters {
    /**
     * The description of the entry filter to be used for user help.
     */
    private String description;

    /**
     * The class name for the entry filter.
     */
    private String entryFilterNames;

    /**
     * The directory for all the entry filter implementations.
     */
    private String entryFiltersDirectory;
}
