library(gridExtra)

manual_dist <- ggplot(manual_predicts, aes(x=predictions * 100)) + 
  geom_histogram(aes(y=..density..),     
                 binwidth=0.0005,
                 colour="black", fill="white") +
  geom_density(alpha=.2, fill="#FF6666") + geom_vline(xintercept = 0.00001224153 * 100, col = 'red') +
  geom_vline(xintercept = 0.002046639 * 100) + labs(title = 'Manual Predictions Dist.') + 
  geom_text(aes(x=0.00001224153 * 100, label="\nLower Bound", y=5), colour="Red", angle=90) +
  geom_text(aes(x=0.002046639 * 100, label="\nUpper Bound", y=5), colour="Red", angle=90)


pricing_dist <- ggplot(pricing_predicts, aes(x=predictions * 100)) + 
  geom_histogram(aes(y=..density..),     
                 binwidth=0.0005,
                 colour="black", fill="white") +
 geom_density(alpha=.2, fill="#FF6666")  + geom_vline(xintercept = 0.00001224153 * 100, col = 'red') +
geom_vline(xintercept = 0.002046639 * 100) + labs(title = 'Snowflake Predictions Dist.') + 
geom_text(aes(x=0.00001224153 * 100, label="\nLower Bound", y=5), colour="Red", angle=90) +
geom_text(aes(x=0.002046639 * 100, label="\nUpper Bound", y=5), colour="Red", angle=90)

grid.arrange(manual_dist, pricing_dist, ncol=1)
