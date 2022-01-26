import React from "react";

export default function FiftyFiftyBlock(props){
    return (
        <div className="flex">
            <div>{ props.content }</div>
            <div>{ props.image }</div>
        </div>
    )
    
};


