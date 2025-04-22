import React from 'react';
import { Button, Heading, HStack } from '@chakra-ui/react'


export default function Actions({ video_id, user_id }: { video_id: string, user_id: string }) {

    function onclick(action: string) {
        fetch(`http://localhost:8000/${action}`, {
            method: "POST",
            body: JSON.stringify({
                video_id: video_id,
                user_id: user_id
            })
        })
    }
    return (
        <HStack gap={4}>
            <Heading as="h2" size="sm">{video_id}-{user_id}</Heading>
            <Button 
                style={{ backgroundColor: "#00FF00" }}
                variant="solid"
                onClick={() => onclick("like")}
                _hover={{ transform: 'scale(1.05)' }}
                transition="all 0.2s"
            >
                Like
            </Button>
            <Button 
                style={{ backgroundColor: "#FF0000" }}
                variant="solid"
                onClick={() => onclick("dislike")}
                _hover={{ transform: 'scale(1.05)' }}
                transition="all 0.2s"
            >
                Dislike
            </Button>
        </HStack>
    );
}