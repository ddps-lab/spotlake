import React from "react";
import {ContentBox, Box, SubTitle, Title, Wrapper, theme, CustomButton, ButtonICon} from "../../UI/styles";
import {ThemeProvider} from "@mui/material/styles";

function Contact () {
    return (
        <Wrapper>
            <Box column align={'flex-start'} gap={1} margin={`20px 0`}>
                <ContentBox className="contact">
                    SpotLake system is maintained by Distributed Data Processing System Lab (DDPS Lab, <a href="https://ddps.cloud" target={"_blank"}>https://ddps.cloud</a>) at Kookmin University.
                    If you have any question, suggestion, or request, you can contact email (<a href="mailto:ddps@kookmin.ac.kr">ddps@kookmin.ac.kr</a>) or create issue on GitHub repository
                    <ThemeProvider theme={theme}>
                        <CustomButton
                            size={"small"}
                            margin={"0 0 0 10px"}
                            startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                            color={"github"}
                            variant="contained"
                            onClick={() => {window.open('https://github.com/ddps-lab/spotlake')}}
                        >Github SpotLake</CustomButton>
                    </ThemeProvider>
                </ContentBox>
            </Box>
            <Box column align={'flex-start'} gap={1} margin={`0`}>
                <ContentBox>
                    <b>Contributing developers (names in the alphabetical order)</b>
                    <ul className={"contribute_developers"}>
                        <li>Chaelim Heo: Front-end development,
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    margin={"0 0 0 10px"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/h0zzae')}}
                                >Github @h0zzae</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li>
                            Hanjeong Lee: Microsoft Azure dataset collection,
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    margin={"0 0 0 10px"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/leehanjeong')}}
                                >Github @leehanjeong</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li>
                            Hyeonyoung Lee: Google Cloud dataset collection,
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    margin={"0 0 0 10px"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/wynter122')}}
                                >Github @wynter122</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li>
                            Jaeil Hwang: Server-side development,
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    margin={"0 0 0 10px"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/chris0765')}}
                                >Github @chris0765</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li>
                            Jungmyeong Park: Front-end development
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    margin={"0 0 0 10px"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/j-myeong')}}
                                >Github @j-myeong</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li>
                            Kyunghwan Kim: Database optimization,
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    margin={"0 0 0 10px"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/red0sena')}}
                                >Github @red0sena</CustomButton>
                            </ThemeProvider>
                        </li>
                        <li>
                            Sungjae Lee: AWS dataset collection,
                            <ThemeProvider theme={theme}>
                                <CustomButton
                                    size={"small"}
                                    margin={"0 0 0 10px"}
                                    startIcon={<ButtonICon src={process.env.PUBLIC_URL + "/icon/GitHub-Mark-120px-plus.png"} />}
                                    color={"github"}
                                    variant="contained"
                                    onClick={() => {window.open('https://github.com/james-sungjae-lee')}}
                                >Github @james-sungjae-lee</CustomButton>
                            </ThemeProvider>
                        </li>
                    </ul>
                </ContentBox>
            </Box>
        </Wrapper>
    )
}
export default Contact;