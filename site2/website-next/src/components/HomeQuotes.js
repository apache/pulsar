import React from "react";

export default function HomeQuotes(props) {
  let quoteList = props.quotes;
  quoteList.sort(() => Math.random() - 0.5);
  const theQuote = quoteList[0];
  
  return (
    <section className="quotes-wrap py-12">
      <div className="inner">
        <div className="single-quote text-center">
          <blockquote className="text-grey-blue">{theQuote.content}</blockquote>
          <div className="text--center relative quote-name mt-4 pt-4">
            <p className="text-grey-blue uppercase"><small>{theQuote.name} | <strong>{theQuote.company}</strong></small></p>
          </div>
        </div>
      </div>
    </section>
  );
}
