import { ChakraProvider } from '@chakra-ui/react'
import { defaultSystem } from "@chakra-ui/react"
import Header from "./components/Header";
import Actions from './components/Actions';

function App() {

  return (
    <ChakraProvider value={defaultSystem}>
      <Header />
      <Actions video_id="123" user_id="u123" />
      <Actions video_id="456" user_id="u456" />
    </ChakraProvider>
  )
}

export default App;