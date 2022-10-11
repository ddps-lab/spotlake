import React from "react";
import {ContentBox, Box, SubTitle, Title, Wrapper} from "../../UI/styles";

function Contact () {
    return (
        <Wrapper padding={`1rem 4rem`}>
            <Box column align={'flex-start'} gap={1} margin={`20px 0`}>
                <ContentBox className="contact">
                    SpotLake system is maintained by Distributed Data Processing System Lab (DDPS Lab, <a href="https://ddps.cloud">https://ddps.cloud</a>) at Kookmin University. If you have any question, suggestion, or request, you can contact email (<a href="mailto:ddps@kookmin.ac.kr">ddps@kookmin.ac.kr</a>) or create issue on GitHub repository (<a href="https://github.com/ddps-lab/spotlake">https://github.com/ddps-lab/spotlake</a>).
                </ContentBox>
            </Box>
            <Box column align={'flex-start'} gap={1} margin={`0`}>
                <ContentBox>
                    <b>Contributing developers (names in the alphabetical order)</b>
                    <ul>
                        <li>Chaelim Heo: Front-end development, <a href="https://github.com/h0zzae">github</a>
                        </li>
                        <li>
                            Hanjeong Lee: Microsoft Azure dataset collection, <a href="https://github.com/">github</a>
                        </li>
                        <li>
                            Hyeonyoung Lee: Google Cloud dataset collection, <a href="https://github.com/">github</a>
                        </li>
                        <li>
                            Jaeil Hwang: Server-side development, <a href="https://github.com/">github</a>
                        </li>
                        <li>
                            Kyunghwan Kim: Database optimization, <a href="https://github.com/">github</a>
                        </li>
                        <li>
                            Sungjae Lee: AWS dataset collection, <a href="https://github.com/james-sungjae-lee">github</a>
                        </li>
                    </ul>
                </ContentBox>
            </Box>
        </Wrapper>
    )
}
export default Contact;