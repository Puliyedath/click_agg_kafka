import { ChakraProvider, Heading } from '@chakra-ui/react'
import { defaultSystem } from "@chakra-ui/react"
import Header from "./components/Header";
import Actions from './components/Actions';
import { useEffect, useState } from 'react';

function App() {

  const [likesData, setLikesData] = useState("");
  useEffect(() =>{
    const eventSource = new EventSource("http://localhost:8000/sse");
    eventSource.onmessage = (event) => {
      console.log("event.data", event.data);
      setLikesData(event.data);
    }
    return () => eventSource.close();
  }, [])
    // useEffect(() => {
    //     const id = setInterval(() => {  
    //         fetch(`http://localhost:8000/top-videos`)
    //             .then(res => res.json())
    //             .then(data => {
    //               console.log("data from top videos", data);
    //                 // setLikesData(JSON.stringify(data));
    //             })
    //     }, 1000)
    //     return () => clearInterval(id)
    // }, [])

  return (
    <ChakraProvider value={defaultSystem}>
      <Header />
      <Actions video_id="123" user_id="u123" />
      <Actions video_id="456" user_id="u456" />
      <Heading as="h2" size="sm">LikesData: {likesData}</Heading>
    </ChakraProvider>
  )
}

export default App;