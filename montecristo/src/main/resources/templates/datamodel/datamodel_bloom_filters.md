## Bloom Filters

Bloom filters are used by {{software}} to quickly determine if a partition key exists in a particular SSTable. The filter is kept in memory (stored off heap), and provides a probabilistic set membership test. It gives either a definitive "does not exist" result or "does exist" with a certain probability. The higher the probability of a false positive, when the filter returns true but the element is not in the set, the larger the bloom filter and the more memory it uses. 

The probability is controlled by the `bloom_filter_fp_chance` table property, which defaults to 0.10 when using LeveledCompactionStrategy and 0.01 when using SizeTieredCompactionStrategy.

The rate of Bloom Filter false positives can be tracked via metrics and using the nodetool cfstats command. Typically we expect the rate to be close to the defined rate in the table schema.

The amount of off-heap memory usage can be viewed per table using nodetool cfstats, or by JMX.

{{overview}}
