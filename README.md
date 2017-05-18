# T2K Match

T2K Match [1] is matching algorithm optimised to match millions of web tables against a central knowledge base.

Many web sites provide data in the form of HTML tables. Millions of such data tables have been extracted from the [CommonCrawl](http://commoncrawl.org/) web corpus by the [Web Data Commons](http://webdatacommons.org/webtables/) project [3]. Data from these tables can be used to fill missing values in large cross-domain knowledge bases such as DBpedia [2]. This project is an example of how pre-defined building blocks from the [WInte.r framework](https://github.com/olehmberg/winter) are combined into an advanced, use-case specific integration method. The algorithm is optimized to match millions of Web tables against a central knowledge base describing millions of instances belonging to hundreds of different classes  (such a people or locations) [2]. 

## Acknowledgments

This project is a re-implementation of the [original T2K Match algorithm](http://dws.informatik.uni-mannheim.de/en/research/T2K) developed at the [Data and Web Science Group](http://dws.informatik.uni-mannheim.de/) at the [University of Mannheim](http://www.uni-mannheim.de/) using the [WInte.r framework](https://github.com/olehmberg/winter). 

## License

T2K Match can be used under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).

## References
[1] Ritze, D., Lehmberg, O., & Bizer, C. (2015, July). Matching html tables to dbpedia. In Proceedings of the 5th International Conference on Web Intelligence, Mining and Semantics (p. 10). ACM.

[2] Ritze, D., Lehmberg, O., Oulabi, Y., & Bizer, C. (2016, April). Profiling the potential of web tables for augmenting cross-domain knowledge bases. In Proceedings of the 25th International Conference on World Wide Web (pp. 251-261). International World Wide Web Conferences Steering Committee.

[3] Lehmberg, O., Ritze, D., Meusel, R., & Bizer, C. (2016, April). A large public corpus of web tables containing time and context metadata. In Proceedings of the 25th International Conference Companion on World Wide Web (pp. 75-76). International World Wide Web Conferences Steering Committee.
